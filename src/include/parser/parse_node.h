/*-------------------------------------------------------------------------
 *
 * parse_node.h
 *
 *
 * Copyright (c) 1994, Regents of the University of California
 *
 * $Id: parse_node.h,v 1.4 1998/01/15 19:00:16 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_NODE_H
#define PARSE_NODE_H

#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#include <nodes/primnodes.h>
#include <nodes/parsenodes.h>
#include <parser/parse_type.h>
#include <utils/rel.h>

typedef struct QueryTreeList
{
	int			len;			/* number of queries */
	Query	  **qtrees;
} QueryTreeList;

/* state information used during parse analysis */
typedef struct ParseState
{
	int			p_last_resno;
	List	   *p_rtable;
	List	   *p_insert_columns;
	bool		p_hasAggs;
	bool		p_is_insert;
	bool		p_is_update;
	bool		p_is_rule;
	bool		p_in_where_clause;
	Relation	p_target_relation;
	RangeTblEntry *p_target_rangetblentry;
} ParseState;

extern ParseState *make_parsestate(void);
extern Expr *make_op(char *opname, Node *ltree, Node *rtree);
extern Var *make_var(ParseState *pstate, char *refname, char *attrname, Oid *type_id);
extern ArrayRef   *make_array_ref(Node *expr,
			   List *indirection);
extern ArrayRef   *make_array_set(Expr *target_expr,
						   List *upperIndexpr,
						   List *lowerIndexpr,
						   Expr *expr);
extern Const *make_const(Value *value);
			   
#endif							/* PARSE_NODE_H */
