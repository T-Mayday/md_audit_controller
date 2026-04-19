STORES= """
        select * from stores
"""

STORES = """
SELECT *
FROM stores
LIMIT 10
"""

USERS = """
SELECT id, full_name, encrypted_inn
FROM users
WHERE id = %s
"""

UPDATE_USER = """
UPDATE users
SET full_name = %s
WHERE id = %s
"""