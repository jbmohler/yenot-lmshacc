import yenot.backend.api as api


TAG_BANK_PENDING = "Bank Pending"
TAG_BANK_RECONCILED = "Bank Reconciled"


def yenot_lmshacc_data_init(conn, args):
    for tag in [TAG_BANK_PENDING, TAG_BANK_RECONCILED]:
        api.sql_void(
            conn, "insert into hacc.tags (tag_name) values (%(t)s)", {"t": tag}
        )
    conn.commit()


api.add_data_init(yenot_lmshacc_data_init)
