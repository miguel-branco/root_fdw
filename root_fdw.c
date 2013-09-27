/*-------------------------------------------------------------------------
 *
 * root_fdw.c
 *		  foreign-data wrapper for server-side ROOT files.
 *
 * Copyright (c) 2013, ...
 *
 * IDENTIFICATION
 *		  contrib/root_fdw/root_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "postmaster/bgworker.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "rootcursor.h"

PG_MODULE_MAGIC;

void _PG_init(void);

/*
 * Location of shards catalog.
 */
static char *ShardPath = NULL;

/*
 * ROOT cursor instance per shard.
 */
#define MAXSHARDS 100

static Root *root_shard[MAXSHARDS];

/*
 * Contains ROOT attribute name and attribute type as defined in the table
 * options.
 */
typedef struct RootAttr
{
	char				*attname;
	RootAttributeType	atttype;
} RootAttr;

/*
 * Contains attributes being requested and their position in the PostgreSQL
 * tuple.
 */
typedef struct QueryAttr
{
	RootAttr	   *attr;
	int				pos;
} QueryAttr;

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * foreign table.
 */
typedef struct RootFdwPlanState
{
	int				shard;			/* shard number */
	char		   *tree;			/* ROOT tree name */
	List		   *schema;			/* ROOT schema defined in 'options' */
	bool			is_collection;	/* is collection? */
	RootTable	   *root_table;		/* ROOT table to read */
	BlockNumber 	pages;			/* estimate of physical size */
	double			ntuples;		/* estimate of number of rows */
} RootFdwPlanState;

/*
 * FDW-specific ignformation for ForeignScanState.fdw_state.
 */
typedef struct RootFdwExecutionState
{
	RootCursor	   *root_cursor;	/* ROOT cursor */
	int			   *pos;			/* List with positions of attributes to project */
	int				nattrs;			/* Number of attributes */
} RootFdwExecutionState;

/*
 * SQL functions
 */
extern Datum root_fdw_handler(PG_FUNCTION_ARGS);
extern Datum root_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(root_fdw_handler);
PG_FUNCTION_INFO_V1(root_fdw_validator);

/*
 * FDW callback routines
 */
static void rootGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid);
static void rootGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid);
static ForeignScan *rootGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses);
static void rootBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *rootIterateForeignScan(ForeignScanState *node);
static void rootReScanForeignScan(ForeignScanState *node);
static void rootEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static char **get_shard_contents(int shard, int *n);
static void rootGetOptions(Oid foreigntableid,
						   int *shard, char **tree,
						   List **schema, bool *is_collection);
static List *collect_attributes(RelOptInfo *baserel,
								RootFdwPlanState *fdw_private,
								Oid foreigntableid);
static void estimate_size(PlannerInfo *root, RelOptInfo *baserel,
			  RootFdwPlanState *fdw_private);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   RootFdwPlanState *fdw_private,
			   Cost *startup_cost, Cost *total_cost);

/*
 * Plugin initializer.
 */
void
_PG_init(void)
{
	int i;

	for (i = 0; i < MAXSHARDS; i++)
	{
		root_shard[i] = NULL;
	}
}

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
root_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = rootGetForeignRelSize;
	fdwroutine->GetForeignPaths = rootGetForeignPaths;
	fdwroutine->GetForeignPlan = rootGetForeignPlan;
	fdwroutine->BeginForeignScan = rootBeginForeignScan;
	fdwroutine->IterateForeignScan = rootIterateForeignScan;
	fdwroutine->ReScanForeignScan = rootReScanForeignScan;
	fdwroutine->EndForeignScan = rootEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses root_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
root_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	int			shard = -1;
	char	   *tree = NULL;
	char	   *collection = NULL;
	int			nattrs = -1;
	ListCell   *cell;

	foreach(cell, options)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "shard") == 0)
		{
			if (shard >= 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: 'shard'")));
			}
			shard = strtod(defGetString(def), NULL);
		}
		else if (strcmp(def->defname, "tree") == 0)
		{
			if (tree)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("conflicting or redundant options: 'tree'")));
			}
			tree = defGetString(def);
		}
		else if (strcmp(def->defname, "collection") == 0)
		{
			if (collection)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("conflicting or redundant options: 'tree'")));
			}
			collection = defGetString(def);
		} else if (strcmp(def->defname, "nattrs") == 0)
		{
			if (nattrs >= 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: 'nattrs'")));
			}
			nattrs = strtod(defGetString(def), NULL);
		}
	}

	/* shard option is required for root_fdw foreign tables */
	if (catalog == ForeignServerRelationId && shard == -1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("'shard' option is required as a root_fdw server option")));
	}
	/* shard option must only be presented as a root_fdw server option */
	else if (catalog != ForeignServerRelationId && shard != -1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("'shard' option can only be used as a root_fdw server option")));
	}

	/* tree option is required for root_fdw foreign tables */
	if (catalog == ForeignTableRelationId && tree == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("'tree' option is required as a root_fdw table option")));
	}
	/* tree option must only be presented as a root_fdw foreign table option */
	else if (catalog != ForeignTableRelationId && tree != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("'tree' option can only be used as a root_fdw table option")));
	}

	/* collection option must only be presented as a root_fdw foreign table option */
	if (catalog != ForeignTableRelationId && collection != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("'collection' option can only be used as a root_fdw table option")));
	}

	/* nattrs option is required for root_fdw foreign tables */
	if (catalog == ForeignTableRelationId && nattrs == -1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("'nattrs' option is required as a root_fdw table option")));
	}
	/* nattrs option must only be presented as a root_fdw foreign table option */
	else if (catalog != ForeignTableRelationId && nattrs != -1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("'nattrs' option can only be used as a root_fdw table option")));
	}

	PG_RETURN_VOID();
}

/*
 * Get shard contents from catalog.
 */
static char
**get_shard_contents(int shard, int *n)
{
	FILE   *f;
	char  **fnames = NULL;
	int		siz = 0;
	char	path[1024];
	char	buf[1024];

	if (ShardPath == NULL)
	{
		ShardPath = getenv("SHARDS_PATH");
		if (ShardPath == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
					 errmsg("SHARDS_PATH environment variable required for root_fdw")));
		}
	}

	sprintf(path, "%s/shard-%d.files", ShardPath, shard);

	f = AllocateFile(path, "r");
	if (!f)
	{
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	*n = 0;
	while (fgets(buf, 1024, f) != NULL)
	{
		char   *pos;
		int		len;

		if ((pos = strchr(buf, '\n')) != NULL)
		{
			*pos = '\0';
		}

		/*
		 * FIXME: Validate whether this is a valid file or not.
		 */
		len = strlen(buf);
		if (len == 0)
		{
			continue;
		}

		if (*n == siz)
		{
			if (siz == 0)
			{
				siz = 2;
				fnames = palloc(siz * sizeof(char *));
			}
			else
			{
				siz *= 2;
				fnames = repalloc(fnames, siz * sizeof(char *));
			}
		}
		fnames[*n] = palloc((len + 1) * sizeof(char));
		strncpy(fnames[*n], buf, len + 1);
		(*n)++;
	}

	FreeFile(f);

	return fnames;
}

/*
 * Fetch the options for a root_fdw foreign table.
 */
static void
rootGetOptions(Oid foreigntableid,
			   int *shard, char **tree,
			   List **schema, bool *is_collection)
{
	ForeignTable 	   *table;
	ForeignServer 	   *server;
	ForeignDataWrapper *wrapper;
	List	   		   *options;
	ListCell   		   *cell;
	char			   *collection = NULL;
	int					nattrs = -1;
	RootAttr		   *attr;
	int					len;

	/*
	 * Default uninitialized values for options.
	 */
	*shard = -1;
	*tree = NULL;
	*schema = NIL;
	*is_collection = false;

	/*
	 * Extract options from FDW objects.  We ignore user mappings because
	 * root_fdw doesn't have any options that can be specified there.
	 *
	 * (XXX Actually, given the current contents of valid_options[], there's
	 * no point in examining anything except the foreign table's own options.
	 * Simplify?)
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	foreach(cell, options)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "shard") == 0)
		{
			*shard = strtod(defGetString(def), NULL);
		}
		else if (strcmp(def->defname, "tree") == 0)
		{
			*tree = pstrdup(defGetString(def));
		}
		else if (strcmp(def->defname, "collection") == 0)
		{
			collection = defGetString(def);
			*is_collection = true;
		}
		else if (strcmp(def->defname, "nattrs") == 0)
		{
			nattrs = strtod(defGetString(def), NULL);
		}
		else if (strncmp(def->defname, "attr_", 5) == 0)
		{
			char *attname;
			char *atttype;
			char *saveptr;

			attname = strtok_r(defGetString(def), ":", &saveptr);
			if (attname == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
						 errmsg("invalid attribute name for option %s in root_fdw", def->defname)));
			}

			atttype = strtok_r(NULL, ":", &saveptr);
			if (atttype == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
						 errmsg("invalid attribute type for option %s in root_fdw", def->defname)));
			}

			attr = (RootAttr *) palloc(sizeof(RootAttr));
			attr->attname = pstrdup(attname);
			attr->atttype = get_root_type(atttype);
			if (attr->atttype == RootInvalidType)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
						 errmsg("invalid attribute type for field %s in root_fdw", def->defname)));
			}

			*schema = lappend(*schema, attr);
		}
	}

	/* Add tree data type to schema */
	attr = (RootAttr *) palloc(sizeof(RootAttr));

	attr->attname = (char *) palloc(strlen(*tree) + 4);
	sprintf(attr->attname, "%s_id", *tree);
	attr->atttype = RootTreeId;

	*schema = lappend(*schema, attr);

	/* Add collection data type to schema */
	if (*is_collection)
	{
		attr = (RootAttr *) palloc(sizeof(RootAttr));
		attr->attname = (char *) palloc(strlen(collection) + 4);
		sprintf(attr->attname, "%s_id", collection);
		attr->atttype = RootCollectionId;

		*schema = lappend(*schema, attr);
	}

	/* Run-time validation of shard */
	if (*shard < 0 || *shard >= MAXSHARDS)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("'shard' option refers to an unknown shard in root_fdw")));
	}

	/* Run-time validation of number of attributes */
	if (nattrs < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("'nattrs' option contains an invalid number of attributes in root_fdw")));
	}
	len = (*is_collection) ? nattrs + 2 : nattrs + 1;
	if (list_length(*schema) != len)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("mismatch between 'nattrs' option and attributes specified as options in root_fdw")));
	}
}

/*
 * rootGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
rootGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	RootFdwPlanState   *fdw_private;
	int					n;
	char			  **fnames;
	int					shard;
	char			   *tree;
	bool				is_collection;
	List			   *schema;

	/* Fetch options */
	rootGetOptions(foreigntableid, &shard, &tree, &schema, &is_collection);

	/* Get shard contents */
	fnames = get_shard_contents(shard, &n);

	/* Initialize ROOT object */
	if (!root_shard[shard])
	{
		root_shard[shard] = init_root(fnames, n);
		if (!root_shard[shard])
		{
			elog(ERROR, "failed to initialize ROOT's shard %d", shard);
		}
	}

	/* Build fdw_private */
	fdw_private = (RootFdwPlanState *) palloc(sizeof(RootFdwPlanState));
	fdw_private->shard = shard;
	fdw_private->tree = tree;
	fdw_private->schema = schema;
	fdw_private->is_collection = is_collection;
	fdw_private->root_table = get_root_table(root_shard[shard], tree, is_collection);
	if (!fdw_private->root_table)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("'name' option refers to an unknown table in root_fdw")));
	}
	baserel->fdw_private = (void *) fdw_private;

	/* Estimate relation size */
	estimate_size(root, baserel, fdw_private);
}

/*
 * rootGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in the order in
 *		the data file.
 */
static void
rootGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	RootFdwPlanState *fdw_private = (RootFdwPlanState *) baserel->fdw_private;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *attrs;
	List	   *private;

	/* Collect attributes used by the query */
	attrs = collect_attributes(baserel, fdw_private, foreigntableid);

	/* Estimate costs */
	estimate_costs(root, baserel, fdw_private,
				   &startup_cost, &total_cost);

	/* Build private list with root_table followed by attrs */
	private = list_make2(fdw_private->root_table, attrs);

	/*
	 * Create a ForeignPath node and add it as only possible path.	We use the
	 * fdw_private list of the path to carry the convert_selectively option;
	 * it will be propagated into the fdw_private list of the Plan node.
	 */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 private));

	/*
	 * If data file was sorted, and we knew it somehow, we could insert
	 * appropriate pathkeys into the ForeignPath node to tell the planner
	 * that.
	 */
}

/*
 * rootGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
rootGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses)
{
	Index		scan_relid = baserel->relid;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							best_path->fdw_private);
}

/*
 * rootBeginForeignScan
 *		Initiate access to the file by creating CopyState
 */
static void
rootBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
	RootTable			   *root_table;
	RootCursor			   *root_cursor;
	RootFdwExecutionState  *festate;
	ListCell   			   *lc;
	List				   *attrs;
	int						nattrs;
	int					   *pos;
	int						i = 0;

	/* First element from private list is root_table */
	root_table = (RootTable *) list_nth(plan->fdw_private, 0);
	attrs = (List *) list_nth(plan->fdw_private, 1);

	/* Create ROOT cursor */
	nattrs = list_length(attrs);
	root_cursor	= init_root_cursor(root_table, nattrs);
	if (!root_cursor)
	{
		elog(ERROR, "failed to initialize ROOT's cursor");
	}

	/* Add attributes to cursor */
	pos = (int *) palloc(nattrs * sizeof(int));
	foreach(lc, attrs)
	{
		QueryAttr *attr = (QueryAttr *) lfirst(lc);

		pos[i] = attr->pos;
		if (!set_root_cursor_attr(root_cursor, i,
								  attr->attr->attname, attr->attr->atttype))
		{
			elog(ERROR, "failed to add attribute to ROOT cursor");
		}
		i++;
	}

	/* Open ROOT cursor */
	if (!open_root_cursor(root_cursor))
	{
		elog(ERROR, "failed to open ROOT cursor");
	}

	/* Save state in node->fdw_state */
	festate = (RootFdwExecutionState *) palloc(sizeof(RootFdwExecutionState));
	festate->root_cursor = root_cursor;
	festate->pos = pos;
	festate->nattrs = nattrs;
	node->fdw_state = (void *) festate;
}

/*
 * rootIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
rootIterateForeignScan(ForeignScanState *node)
{
	RootFdwExecutionState  *festate = (RootFdwExecutionState *) node->fdw_state;
	TupleTableSlot 		   *slot = node->ss.ss_ScanTupleSlot;
	Datum				   *values = slot->tts_values;
	bool				   *nulls = slot->tts_isnull;
	RootCursor			   *root_cursor = festate->root_cursor;
	int					   *pos = festate->pos;
	int						nattrs = festate->nattrs;
	int 					i = 0;

	/*
	 * The protocol for loading a virtual tuple into a slot is first
	 * ExecClearTuple, then fill the values/isnull arrays, then
	 * ExecStoreVirtualTuple.  If we don't find another row in the file, we
	 * just skip the last step, leaving the slot empty as required.
	 *
	 * We can pass ExprContext = NULL because we read all columns from the
	 * file, so no need to evaluate default expressions.
	 *
	 * We can also pass tupleOid = NULL because we don't allow oids for
	 * foreign tables.
	 */
	ExecClearTuple(slot);

	if (advance_root_cursor(root_cursor))
	{
		/* Initialize all values for this row to null */
		//memset(values, 0, nattrs * sizeof(Datum));
		//memset(nulls, true, nattrs * sizeof(bool));

		/* Save values to tuple */
		for (i = 0; i < nattrs; i++)
		{
			int p = pos[i];
			nulls[p] = false;

			switch (get_root_cursor_attr_type(root_cursor, i))
			{
			case RootTreeId:
				values[p] = Int64GetDatum(get_tree_id(root_cursor, i));
				break;
			case RootCollectionId:
				values[p] = Int32GetDatum(get_collection_id(root_cursor, i));
				break;
			case RootInt:
				values[p] = Int32GetDatum(get_int(root_cursor, i));
				break;
			case RootUInt:
				values[p] = UInt32GetDatum(get_uint(root_cursor, i));
				break;
			case RootFloat:
				values[p] = Float8GetDatum(get_float(root_cursor, i));
				break;
			case RootBool:
				values[p] = BoolGetDatum(get_bool(root_cursor, i));
				break;
			default:
				elog(ERROR, "ROOT invalid type found");
				break;
			}
		}
		ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * rootReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
rootReScanForeignScan(ForeignScanState *node)
{
	//RootFdwExecutionState *festate = (RootFdwExecutionState *) node->fdw_state;
	// FIXME: Not implemented.
}

/*
 * rootEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
rootEndForeignScan(ForeignScanState *node)
{
	RootFdwExecutionState *festate = (RootFdwExecutionState *) node->fdw_state;
	fini_root_cursor(festate->root_cursor);
}

/*
 * Collect all attributes needed by query.
 *
 * The attributes collected are those needed by joins, final output or used by
 * restriction clauses.
 */
static List
*collect_attributes(RelOptInfo *baserel, RootFdwPlanState *fdw_private,
					Oid foreigntableid)
{
	bool		has_wholerow = false;
	ListCell   *lc;
	Bitmapset  *attrs_used = NULL;
	Relation	rel;
	TupleDesc	tupdesc;
	List	   *attrs = NIL;
	int			i;
	QueryAttr	*qattr;

	/* Collect all the attributes needed for joins or final output. */
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid,
				   &attrs_used);

	/* Add all the attributes used by restriction clauses. */
	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &attrs_used);
	}

	/* If there's a whole-row reference, we'll need all the columns. */
	has_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								 attrs_used);

	/* Build List with attribute numbers. */
	rel = heap_open(foreigntableid, AccessShareLock);
	tupdesc = RelationGetDescr(rel);
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = tupdesc->attrs[i - 1];

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (has_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			qattr = NULL;
			/*
			 * Find requested attribute in the set of attributes given as
			 * options in the table.
			 */
			foreach(lc, fdw_private->schema)
			{
				RootAttr *rattr = (RootAttr *) lfirst(lc);

				if (strcasecmp(rattr->attname, attr->attname.data) == 0)
				{
					qattr = (QueryAttr *) palloc(sizeof(QueryAttr));
					qattr->attr = rattr;
					qattr->pos = attr->attnum - 1;
					break;
				}
			}

			if (qattr == NULL)
			{
				elog(ERROR,
					"Failed to retrieve ROOT attribute %s in ROOT schema",
					attr->attname.data);
			}
			attrs = lappend(attrs, qattr);
		}
	}
	heap_close(rel, AccessShareLock);

	return attrs;
}

/*
 * Estimate size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
static void
estimate_size(PlannerInfo *root, RelOptInfo *baserel,
			  RootFdwPlanState *fdw_private)
{
	BlockNumber pages;
	double		ntuples;
	double		nrows;
	double		fsize;

	/* Get size estimate from ROOT. */
	ntuples = get_root_table_approx_size(fdw_private->root_table);
	fdw_private->ntuples = ntuples;

	/*
	 * Convert the number of tuples to an estimate of the I/O cost.
	 * Needless to say, this is completely bogus!
	 */
	fsize = ntuples * 100;
	pages = (fsize + (BLCKSZ - 1)) / BLCKSZ;
	if (pages < 1)
		pages = 1;
	fdw_private->pages = pages;

	/*
	 * Now estimate the number of rows returned by the scan after applying the
	 * baserestrictinfo quals.
	 */
	nrows = ntuples *
		clauselist_selectivity(root,
							   baserel->baserestrictinfo,
							   0,
							   JOIN_INNER,
							   NULL);

	nrows = clamp_row_est(nrows);

	/* Save the output-rows estimate for the planner */
	baserel->rows = nrows;
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   RootFdwPlanState *fdw_private,
			   Cost *startup_cost, Cost *total_cost)
{
	BlockNumber pages = fdw_private->pages;
	double		ntuples = fdw_private->ntuples;
	Cost		run_cost = 0;
	Cost		cpu_per_tuple;

	/*
	 * We estimate costs almost the same way as cost_seqscan(), thus assuming
	 * that I/O costs are equivalent to a regular table file of the same size.
	 * However, we take per-tuple CPU costs as 1.5x of a seqscan, to account
	 * for the cost of moving data from ROOT to PostgreSQL tuples.
	 */
	run_cost += seq_page_cost * pages;

	*startup_cost = baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost * 1.5 + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * ntuples;
	*total_cost = *startup_cost + run_cost;
}
