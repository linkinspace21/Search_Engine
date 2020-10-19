import sqlite3

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

# Find the ids that send out page rank - we only are interested
# in pages in the SCC that have in and out links
cur.execute('''SELECT DISTINCT from_id FROM Links''')
from_ids = list()
for row in cur:
    from_ids.append(row[0])

# Find the ids that receive page rank
to_ids = list()
links = list()
cur.execute('''SELECT DISTINCT from_id, to_id FROM Links''')
for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id:
        continue
    if from_id not in from_ids:
        continue
    if to_id not in from_ids:
        continue
    links.append(row)
    if to_id not in to_ids:
        to_ids.append(to_id)

# Get latest page ranks for stronger connection
prev_ranks = dict()
for node in from_ids:
    cur.execute('''SELECT new_rank FROM Pages WHERE id = ?''', (node, ))
    row = cur.fetchone()
    prev_ranks[node] = row[0]

sval = input('How many iterations:')
many = 1
if (len(sval) > 0):
    many = int(sval)

if len(prev_ranks) < 1:
    print("Nothing to page rank.  Check data.")
    quit()

# Ranking in memory
for i in range(many):
    next_ranks = dict()
    total = 0.0
    for (node, old_rank) in list(prev_ranks.items()):
        total = total + old_rank
        next_ranks[node] = 0.0

    # Find the number of outbound links and sent the page rank down each
    for (node, old_rank) in list(prev_ranks.items()):
        give_ids = list()
        for (from_id, to_id) in links:
            if from_id != node:
                continue

            if to_id not in to_ids:
                continue
            give_ids.append(to_id)
        if (len(give_ids) < 1):
            continue
        amount = old_rank / len(give_ids)

        for id in give_ids:
            next_ranks[id] = next_ranks[id] + amount

    newtot = 0
    for (node, next_rank) in list(next_ranks.items()):
        newtot = newtot + next_rank
    evap = (total - newtot) / len(next_ranks)

    for node in next_ranks:
        next_ranks[node] = next_ranks[node] + evap

    newtot = 0
    for (node, next_rank) in list(next_ranks.items()):
        newtot = newtot + next_rank

    # per-page average change from old rank to new rank
    # Do it as many times to see it converge
    totdiff = 0
    for (node, old_rank) in list(prev_ranks.items()):
        new_rank = next_ranks[node]
        diff = abs(old_rank - new_rank)
        totdiff = totdiff + diff

    avediff = totdiff / len(prev_ranks)
    # Use if you want to see for each iteration
    # print(i + 1, avediff)

    # rotate
    prev_ranks = next_ranks

# final ranks back into the database
print(list(next_ranks.items()))
cur.execute('''UPDATE Pages SET old_rank=new_rank''')
for (id, new_rank) in list(next_ranks.items()):
    cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (new_rank, id))
conn.commit()
cur.close()
