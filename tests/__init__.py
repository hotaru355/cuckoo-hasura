import os

os.environ["HASURA_URL"] = "http://localhost:8080/v1/graphql"
os.environ["HASURA_ROLE"] = "admin"
os.environ["HASURA_ADMIN_SECRET"] = "hasura"
