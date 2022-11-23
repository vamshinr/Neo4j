from neo4j import GraphDatabase
import pandas as pd
#AURA_CONNECTION_URI = "neo4j+ssc://762d63d9.databases.neo4j.io"
AURA_CONNECTION_URI = "neo4j+ssc://3e5327eb.databases.neo4j.io"
AURA_USERNAME = "neo4j"
#AURA_PASSWORD = "3Bp6jyD1v_8WwXG3YwBADWruXCpVarrbdRyMi5ImOJU"
AURA_PASSWORD = "oN1jREJfpVn_5MaRT44BuewTH5DKhITftTUchiKJMQk"
# Driver instantiation
driver = GraphDatabase.driver(
    AURA_CONNECTION_URI,
    auth=(AURA_USERNAME, AURA_PASSWORD)
)

# merge (c:Company{name:'amazon',country:'US',scale:'10,001+ employees',industry:'Technology, Information and Internet',description:''})
# merge (j:Job{name:'software development engineer intern',company:'amazon',city:'san jose',state:'CA',degree:'masters',experience:1,skills:['c','java','python'],description:''})

df = pd.read_csv("/Users/vamshireddy/Downloads/FALL2022/csc244/sanjosejobs.csv")
jobname, city, company, country, state, degree, exp = "", "", "", "", "", "",-1
skills = []
description = ""

for i in range(len(df)):
    description = df.loc[i]['snippet']
    jobname, city = df.loc[i]['jobtitle'].lower(), df.loc[i]['city'].lower()
    company, country, state = df.loc[i]['company'].lower(), df.loc[i]['country'].lower(), df.loc[i]['state'].lower()
    if "entry" in jobname:
        degree,exp = "bachelors",0
    elif ("research" in jobname) or ("r&d" in jobname) or ("doctorate" in jobname):
        degree,exp = "phd",3
    elif ("sr" in jobname) or ("senior" in jobname) or ("principal" in jobname) or ("staff" in jobname) or ("mgr" in jobname):
        degree,exp = "masters",3
    else:
        degree,exp = "bachelors",1

    if "data" in jobname or "scientist" in jobname:
        skills = ['python','machine learning','flask','hadoop','natural language processing']
    else:
        skills = ['java','c','python',"datastructures"]
    querys = []
    # for i in range(len(skills)):
    #     querys.append(f"MERGE (j:Job{{name:\"{jobname}\"}}) MERGE "
    #                   f"(s:Skill{{name:\"{skills[i]}\"}}) MERGE(j)-[:REQ_SKILL]->(s) return j,s")

    querys.append(f"merge (j: Job{{name:\"{jobname}\", company: \"{company}\", "
                  f"city: \"{city}\", state: \"{state}\", degree: \"{degree}\", experience: {exp}, skills: {skills}, "
                  f"description: \"{description}\"}})")
    with driver.session() as session:
        for q in querys:
            print(session.run(q).data())

driver.close()