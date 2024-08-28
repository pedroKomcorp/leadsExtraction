import json
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('../leads.db')
cur = conn.cursor()

# Execute the query to fetch taxId and members
cur.execute('SELECT taxId, members, company_name FROM empresa')
all_members_group = cur.fetchall()

# Iterate through the fetched data
for i, (taxId, members, companyName) in enumerate(all_members_group):
    members_array = []

    # Parse the JSON-formatted string
    members_data = json.loads(members)
    if not members_data:
        print('No members found')
        members_array.append('No members found')
    else:
        for member in members_data:
            pessoa_cargo = member['person']['name'] + ' - ' + member['role']['text'] + '| '
            members_array.append(pessoa_cargo)

    # Join the list into a single string
    members_string = ''.join(members_array)

    # Update the row in the database with the new string
    cur.execute('UPDATE empresa SET members = ? WHERE taxId = ? and company_name = ?', (members_string, taxId, companyName))
    print(f'Updated members for {taxId} - {companyName}')
# Commit the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
