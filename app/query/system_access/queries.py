SELECT_EXISTING_SMSTORES = """
SELECT smstore
FROM stores
WHERE smstore IS NOT NULL
"""

INSERT_STORE = """
INSERT INTO stores (
    region,
    smstore,
    name,
    address,
    close_date,
    ukm4store,
    ukm4ip,
    ukm5store,
    latitude,
    longitude
)
VALUES (
    %(region)s,
    %(smstore)s,
    %(name)s,
    %(address)s,
    %(close_date)s,
    %(ukm4store)s,
    %(ukm4ip)s,
    %(ukm5store)s,
    %(latitude)s,
    %(longitude)s
)
"""
GET_USER = """
SELECT *
FROM users
WHERE employee_id = %(employee_id)s
LIMIT 1
"""