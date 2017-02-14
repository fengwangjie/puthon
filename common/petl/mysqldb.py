import sys
import traceback
from collections import namedtuple

from logbook import Logger
from petl.io.db import SQL_INSERT_QUERY
from petl.io.db_utils import _quote

log = Logger('mysqldb')
debug = log.debug

Result = namedtuple('Result', ['total', 'error'])


def upsertdb(table, connection, table_name, key, commit=False, strict=True):
    """Currently only support mysql"""
    total = 0
    error = 0

    keys = key if isinstance(key, tuple) else (key,)

    # sanitise table and field name
    table_name = _quote(table_name)
    it = iter(table)

    fields = it.__next__()
    field_names = map(str, fields)
    col_names = [_quote(n) for n in field_names]
    key_cols = [_quote(n) for n in keys]
    log.debug('column names: {0}', col_names)
    log.debug('key column names: {0} ', key_cols)

    # determine paramstyle and build queries
    insert_query = create_insert_query(connection, table_name, col_names)
    update_query = create_update_query(connection, table_name, col_names, key_cols)
    update_param_indexes = [fields.index(f) for f in fields + keys]
    log.debug('insert query: {0}', insert_query)
    log.debug('update query: {0}', update_query)
    #
    # header = next(it)
    # columns = [_quote(f) for f in map(str, header)]
    # query = _create_upsert_query(table_name, columns)

    log.info('get a cursor')

    for row in it:
        total += 1
        try:
            params = tuple([row[i] for i in update_param_indexes])
            cursor = connection.cursor()
            cursor.execute(update_query, params)
            if not cursor.rowcount:  # insert if not found
                cursor.execute(insert_query, row)

            if commit:
                # log.info('commit transaction')
                connection.commit()
        except:
            type, value, stacktrace = sys.exc_info()
            if strict:
                log.info('an error occurred in strict mode, rollback transaction and close cursor')
                connection.rollback()
                cursor.close()
                raise ValueError(('Failed to save record', row, value)).with_traceback(stacktrace)
            else:
                log.warn("failed to save record to {0}, {1}, {2}", table_name, row, traceback.format_exc(stacktrace))
                error += 1

    log.info('upsertdb complete')
    return Result(total, error)


def create_insert_query(connection, table_name, cols):
    placeholders = param_placeholders(connection, cols)
    insert_col_names = ', '.join(cols)
    insert_query = SQL_INSERT_QUERY % (table_name, insert_col_names, ', '.join(placeholders))
    return insert_query


def create_update_query(connection, table_name, cols, key_cols):
    placeholders = param_placeholders(connection, cols + key_cols)
    set_clause_pairs = zip(cols, placeholders[:len(cols)])
    condition_pairs = zip(key_cols, placeholders[len(cols):])
    set_clause = ', '.join([col + ' = ' + placeholder for col, placeholder in set_clause_pairs])
    search_condition = ' and '.join([col + ' = ' + placeholder for col, placeholder in condition_pairs])
    update_query = 'update %s set %s where %s' % (table_name, set_clause, search_condition)
    return update_query


def param_placeholders(connection, names):
    # discover the paramstyle
    if connection is None:
        # default to using question mark
        debug('connection is None, default to using qmark paramstyle')
        return ['?'] * len(names)
    else:
        mod = __import__(connection.__class__.__module__)

        if not hasattr(mod, 'paramstyle'):
            debug('module %r from connection %r has no attribute paramstyle, '
                  'defaulting to qmark', mod, connection)
            # default to using question mark
            return ['?'] * len(names)

        elif mod.paramstyle == 'qmark':
            debug('found paramstyle qmark')
            return ['?'] * len(names)

        elif mod.paramstyle in ('format', 'pyformat'):
            debug('found paramstyle pyformat')
            return ['%s'] * len(names)

        elif mod.paramstyle == 'numeric':
            debug('found paramstyle numeric')
            return [':' + str(i + 1) for i in range(len(names))]

        else:
            debug('found unexpected paramstyle %r, defaulting to qmark',
                  mod.paramstyle)
            return ['?'] * len(names)
