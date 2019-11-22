import uuid
from bottle import request
import yenot.backend.api as api

app = api.get_global_app()

@app.get('/api/accounts/by-reference', name='get_api_accounts_by_reference')
def get_api_accounts_by_reference():
    reference = request.query.get('reference')

    select = """
select accounts.id, 
    accounttypes.atype_name as type, 
    accounttypes.balance_sheet,
    accounttypes.debit,
    accounts.acc_name as account,
    accounts.description
from hacc.accounts
join hacc.accounttypes on accounttypes.id=accounts.type_id
where acc_name=%(ref)s"""

    params = {'ref': reference}

    results = api.Results()
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                id=api.cgen.pyhacc_account.surrogate(),
                account=api.cgen.pyhacc_account.name(url_key='id', represents=True))
        results.tables['account', True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()

@app.get('/api/accounts/completions', name='get_api_accounts_completions')
def get_api_accounts_completions():
    prefix = request.query.get('prefix')

    select = """
select accounts.id, 
    accounttypes.atype_name as type, 
    accounts.acc_name,
    accounts.description
from hacc.accounts
join hacc.accounttypes on accounttypes.id=accounts.type_id
where acc_name ilike %(p)s
order by acc_name"""

    params = {
            'p': api.sanitize_prefix(prefix)}

    results = api.Results()
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                id=api.cgen.pyhacc_account.surrogate(),
                account=api.cgen.pyhacc_account.name(url_key='id', represents=True))
        results.tables['accounts', True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()

def get_api_accounts_list_prompts():
    return api.PromptList(
            journal=api.cgen.pyhacc_journal.id(label='Journal', widget_kwargs={'all_option': True}),
            acctype=api.cgen.pyhacc_accounttype.id(label='Account Type', widget_kwargs={'all_option': True}),
            __order__=['journal', 'acctype'])

@app.get('/api/accounts/list', name='get_api_accounts_list', \
        report_prompts=get_api_accounts_list_prompts,
        report_title='Account List')
def get_api_accounts_list():
    acctype = request.query.get('acctype', None)
    journal = request.query.get('journal', None)

    select = """
select accounts.id, 
    accounts.acc_name as account,
    accounttypes.id as atype_id,
    accounttypes.atype_name as type, 
    journals.id as jrn_id,
    journals.jrn_name as journal,
    accounts.description
from hacc.accounts
join hacc.accounttypes on accounttypes.id=accounts.type_id
join hacc.journals on journals.id=accounts.journal_id
where /*WHERE*/"""

    wheres = []
    params = {}
    if acctype != None and acctype != '__all__':
        params['ataid'] = acctype
        wheres.append("accounts.type_id=%(ataid)s")
    if journal != None and journal != '__all__':
        params['jrnid'] = journal
        wheres.append("accounts.journal_id=%(jrnid)s")
    if len(wheres) == 0:
        wheres.append("True")

    select = select.replace("/*WHERE*/", " and ".join(wheres))

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                id=api.cgen.pyhacc_account.surrogate(),
                account=api.cgen.pyhacc_account.name(url_key='id', represents=True),
                atype_id=api.cgen.pyhacc_accounttype.surrogate(),
                type=api.cgen.pyhacc_accounttype.name(url_key='atype_id'),
                jrn_id=api.cgen.pyhacc_journal.surrogate(),
                journal=api.cgen.pyhacc_journal.name(url_key='jrn_id'))
        results.tables['accounts', True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()

def _get_api_account(a_id=None, newrow=False):
    select = """
select *
from hacc.accounts
where /*WHERE*/"""

    wheres = []
    params = {}
    if a_id != None:
        params['i'] = a_id
        wheres.append("accounts.id=%(i)s")
    if newrow:
        wheres.append("False")

    assert len(wheres) == 1
    select = select.replace("/*WHERE*/", wheres[0])

    results = api.Results()
    with app.dbconn() as conn:
        columns, rows = api.sql_tab2(conn, select, params)

        if newrow:
            def default_row(index, row):
                row.id = str(uuid.uuid1())
            rows = api.tab2_rows_default(columns, [None], default_row)

        results.tables['account', True] = columns, rows
    return results

@app.get('/api/account/<a_id>', name='get_api_account')
def get_api_account(a_id):
    results = _get_api_account(a_id)
    return results.json_out()

@app.get('/api/account/new', name='get_api_account_new')
def get_api_account_new():
    results = _get_api_account(newrow=True)
    results.keys['new_row'] = True
    return results.json_out()

@app.put('/api/account/<acnt_id>', name='put_api_account')
def put_account(acnt_id):
    acc = api.table_from_tab2('account', amendments=['id'], allow_extra=True)

    if len(acc.rows) != 1:
        raise api.UserError('invalid-input', 'There must be exactly one row.')

    for row in acc.rows:
        row.id = acnt_id

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows('hacc.accounts', acc)
        conn.commit()

    return api.Results().json_out()
