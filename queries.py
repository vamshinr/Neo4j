
def que1(name):
    q = f"MATCH (has_s:Skill)<-[has:HAS_SKILL]-(p:Person{{name:\"{name}\"}})-[:LIVESIN_CITY]" \
                "->(c:City)<-[:IN_CITY]-(j:Job)-[req:REQ_SKILL]->(req_s:Skill) " \
                "optional MATCH (p)-[:HAS_SKILL]->(common:Skill)<-[:REQ_SKILL]-(j) " \
                "with j.name as jobname, c.name as cityname, count(DISTINCT has_s.name) " \
                "as counthasskill, count(DISTINCT req_s.name) as countreqskill, collect(DISTINCT has_s.name)" \
            " as hasskills, collect(DISTINCT req_s.name) as reqskills, collect(DISTINCT common.name) as " \
            "matching_skills, count(DISTINCT common.name) as match_count return jobname,cityname,hasskills," \
        "reqskills,matching_skills,match_count,[x in reqskills WHERE not(x in hasskills)] as missing order " \
                "by match_count desc"
    return q