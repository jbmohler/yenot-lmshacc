import uuid
import yenot.backend.api as api

app = api.get_global_app()

@app.get('/api/accounttypes/list', name='get_api_accounttypes_list', \
        report_title='Account Types List')
def get_api_accounttypes_list():
    select = """
select *
from hacc.accounttypes"""

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(
                id=api.cgen.pyhacc_accounttype.surrogate(),
                atype_name=api.cgen.pyhacc_accounttype.name(label='Type'))
        results.tables['accounttypes', True] = api.sql_tab2(conn, select, column_map=cm)
    return results.json_out()

@app.get('/api/accounttype/new', name='get_api_accounttype_new')
def api_accounttype_new():
    select = """
select *
from hacc.accounttypes
where false"""

    results = api.Results()
    with app.dbconn() as conn:
        columns, rows = api.sql_tab2(conn, select)
        def default_row(index, row):
            row.id = str(uuid.uuid1())
        rows = api.tab2_rows_default(columns, [None], default_row)
        results.tables['accounttype', True] = columns, rows
    return results.json_out()

@app.get('/api/accounttype/<atype_id>', name='get_api_accounttype')
def api_accounttype(atype_id):
    select = """
select *
from hacc.accounttypes
where id=%(at)s"""

    results = api.Results()
    with app.dbconn() as conn:
        results.tables['accounttype', True] = api.sql_tab2(conn, select, {'at': atype_id})
    return results.json_out()

@app.put('/api/accounttype/<atype_id>', name='put_api_accounttype')
def put_accounttype(atype_id):
    atype = api.table_from_tab2('accounttype', amendments=['id'], allow_extra=True)

    if len(atype.rows) != 1:
        raise api.UserError('invalid-argument', 'Exactly one account type must be specified.')

    for row in atype.rows:
        row.id = atype_id

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows('hacc.accounttypes', atype)
        conn.commit()

    return api.Results().json_out()
