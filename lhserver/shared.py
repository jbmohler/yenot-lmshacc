import yenot.backend.api as api


def account_sidebar(idcolumn):
    return [{"name": "account_general", "on_highlight_row": {"id": idcolumn}}]


def enrich_meta(meta):
    meta = meta.copy()
    if meta and "type" in meta:
        mt = meta["type"]
        if mt == "pyhacc_journal.name" and "char_width" not in meta:
            meta["char_width"] = 10
        if mt == "pyhacc_account.name" and "char_width" not in meta:
            meta["char_width"] = 12
        if mt == "pyhacc_accounttype.name" and "char_width" not in meta:
            meta["char_width"] = 10
    return meta


def HaccColumnMap(**kwargs):
    enriched = {attr: enrich_meta(meta) for attr, meta in kwargs.items()}
    return api.ColumnMap(**enriched)


def hacc_columns(columns):
    return [(attr, enrich_meta(meta)) for attr, meta in columns]
