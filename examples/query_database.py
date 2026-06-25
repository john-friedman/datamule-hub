from datamulehub import databases


databases.query(
    "SELECT * FROM simple_xbrl LIMIT 10",
    output_dir="simple_xbrl_sample",
)

rows = databases.read_query("SELECT accessionnumber, filingdate FROM submissions_metadata LIMIT 10000")
print(rows[0])
