from flask import Flask,redirect,url_for,request,render_template
app = Flask(__name__)

from neo4j import GraphDatabase
import pandas as pd
AURA_CONNECTION_URI = "neo4j+ssc://762d63d9.databases.neo4j.io"
AURA_USERNAME = "neo4j"
AURA_PASSWORD = "3Bp6jyD1v_8WwXG3YwBADWruXCpVarrbdRyMi5ImOJU"

# Driver instantiation
driver = GraphDatabase.driver(
    AURA_CONNECTION_URI,
    auth=(AURA_USERNAME, AURA_PASSWORD)
)

@app.route('/')
def index():
    q = "match(p: Person) return p"
    with driver.session() as session:
        res = session.run(q).data()
    res = [r['p']['name'] for r in res]
    print(res)
    return render_template('jobs.html',res=res)

@app.route('/recommend')
def recopage():
    return render_template('recommend.html')

@app.route('/recommendations',methods=['POST'])
def recommendations():
    name = request.form['user']
    skills = request.form['skills'].split(',')
    exp = int(request.form['exp'])
    edu = request.form['edu']
    city = request.form['city']
    queries=[]
    for s in skills:
        queries.append(f"merge (p:Person{{name:\"{name}\"}}) merge (s:Skill{{name:\"{s}\"}}) merge (p)-[:HAS_SKILL]->(s)")
    queries.append(f"merge (p:Person{{name:\"{name}\"}}) merge (e:Experience{{name:{exp}}}) merge (p)-[:HAS_EXPERIENCE]->(e)")
    queries.append(f"merge (p:Person{{name:\"{name}\"}}) merge (d:Degree{{name:\"{edu}\"}}) merge (p)-[:HAS_DEGREE]->(d)")
    queries.append(f"merge (p:Person{{name:\"{name}\"}}) merge (c:City{{name:\"{city}\"}}) merge (p)-[:LIVESIN_CITY]->(c)")
    with driver.session() as session:
        for qu in queries:
            session.run(qu).data()
        temp=f"MATCH (n:Person{{name:\"{name}\"}})"\
                "MATCH (n)-[r]-(o)"\
                "RETURN n as node, collect(o), collect(labels(o)), collect(TYPE(r))"
        res = session.run(temp).data()
        print(res)
    with driver.session() as session:
        q = f"MATCH (has_s:Skill)<-[has:HAS_SKILL]-(p:Person{{name:\"{name}\"}})-[:LIVESIN_CITY]" \
            "->(c:City)<-[:IN_CITY]-(j:Job)-[req:REQ_SKILL]->(req_s:Skill) " \
            "optional MATCH (p)-[:HAS_SKILL]->(common:Skill)<-[:REQ_SKILL]-(j) " \
            "with j.name as jobname, c.name as cityname, count(DISTINCT has_s.name) " \
            "as counthasskill, count(DISTINCT req_s.name) as countreqskill, collect(DISTINCT has_s.name)" \
            " as hasskills, collect(DISTINCT req_s.name) as reqskills, collect(DISTINCT common.name) as " \
            "matching_skills, count(DISTINCT common.name) as match_count return jobname,cityname,hasskills," \
            "reqskills,matching_skills,match_count,[x in reqskills WHERE not(x in hasskills)] as missing order " \
            "by match_count desc"
        res = session.run(q).data()
    return render_template('table.html',res=res)


@app.route('/search',methods=['GET','POST'])
def search():
    if request.method == "POST":
        print(request)
        name = request.form['user']
        with driver.session() as session:
            q = f"MATCH (has_s:Skill)<-[has:HAS_SKILL]-(p:Person{{name:\"{name}\"}})-[:LIVESIN_CITY]" \
                "->(c:City)<-[:IN_CITY]-(j:Job)-[req:REQ_SKILL]->(req_s:Skill) " \
                "optional MATCH (p)-[:HAS_SKILL]->(common:Skill)<-[:REQ_SKILL]-(j) " \
                "with j.name as jobname, c.name as cityname, count(DISTINCT has_s.name) " \
                "as counthasskill, count(DISTINCT req_s.name) as countreqskill, collect(DISTINCT has_s.name)" \
                " as hasskills, collect(DISTINCT req_s.name) as reqskills, collect(DISTINCT common.name) as " \
                "matching_skills, count(DISTINCT common.name) as match_count return jobname,cityname,hasskills," \
                "reqskills,matching_skills,match_count,[x in reqskills WHERE not(x in hasskills)] as missing order " \
                "by match_count desc"
            res = session.run(q).data()
            for r in res:
                r.pop("hasskills")
                r.pop("matching_skills")
                r.pop("match_count")
            print(res)
        return render_template('table.html', res=res, name=name)
    q = "match(p: Person) return p"
    with driver.session() as session:
        res = session.run(q).data()
    return render_template('jobs.html',res=[r['p']['name'] for r in res])


if __name__ == '__main__':
    app.run(host="0.0.0.0",port=5001,debug=True)