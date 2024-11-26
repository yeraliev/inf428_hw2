import random
import unittest
import pandas as pd
import os
from elasticsearch import Elasticsearch, exceptions

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

class Department:
    def __init__(self, name, users, threat_scores):
        self.name = name
        self.users = users
        self.threat_scores = threat_scores

    def calc_mean(self):
        return sum(self.threat_scores) / len(self.threat_scores) if self.threat_scores else 0

class Company:
    def __init__(self, departments):
        self.departments = departments

    def aggregatedThreatScore(self):
        total_weighted_score = 0
        for dep in self.departments:
            mean_score = dep.calc_mean()
            weighted_score = mean_score
            total_weighted_score += weighted_score
        return total_weighted_score

def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

def read_from_csv(filename):
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        return None

def create_elasticsearch_index(index_name):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body={
            "mappings": {
                "properties": {
                    "department": {"type": "keyword"},
                    "threat_scores": {"type": "nested", "properties": {"score": {"type": "integer"}}}
                }
            }
        })

def populate_elasticsearch_index_from_csv(index_name, csv_filename):
    data = read_from_csv(csv_filename)
    if data is not None:
        for i, row in data.iterrows():
            department_name = row['Department']
            threat_scores = eval(row['Threat_Scores'])
            es.index(index=index_name, body={
                "department": department_name,
                "threat_scores": [{"score": score} for score in threat_scores]
            })

def fetch_threat_scores_from_elasticsearch(index_name, department_name):
    query = {
        "query": {
            "match": {
                "department": department_name
            }
        }
    }
    try:
        res = es.search(index=index_name, body=query, size=1)
        if res['hits']['hits']:
            scores = [hit['_source']['threat_scores'] for hit in res['hits']['hits']]
            return [score['score'] for score in scores[0]]
        else:
            return []
    except exceptions.NotFoundError:
        return []

class TestCompanyThreatScore(unittest.TestCase):
    def generate_threat_scores(self, users, score_range=(0, 90)):
        return [random.randint(*score_range) for _ in range(users)]

    def test_oneHighMean(self):
        filename = "test_oneHighMean.csv"
        data = read_from_csv(filename)
        
        if data is None:
            users = 10
            engineering = Department("Engineering", users=users, threat_scores=self.generate_threat_scores(users, (20, 30)))
            marketing = Department("Marketing", users=users, threat_scores=self.generate_threat_scores(users, (20, 30)))
            finance = Department("Finance", users=users, threat_scores=self.generate_threat_scores(users, (20, 30)))
            hr = Department("HR", users=users, threat_scores=self.generate_threat_scores(users, (20, 30)))
            science = Department("Science", users=users, threat_scores=self.generate_threat_scores(users, (80, 90)))
            
            company = Company([engineering, marketing, finance, hr, science])
            aggregated_score = company.aggregatedThreatScore()

            print("One department has a high threat score, others low:", aggregated_score)

            data = {
                'Department': ['Engineering', 'Marketing', 'Finance', 'HR', 'Science'],
                'Threat_Scores': [engineering.threat_scores, marketing.threat_scores, finance.threat_scores, hr.threat_scores, science.threat_scores],
                'Aggregated_Score': [aggregated_score] * 5
            }
            save_to_csv(data, filename)
        else:
            print("Reading from existing CSV file")
            print(data)

    def test_sameMean(self):
        filename = "test_sameMean.csv"
        data = read_from_csv(filename)
        
        if data is None:
            users = 10
            engineering = Department("Engineering", users=users, threat_scores=self.generate_threat_scores(users, (30, 35)))
            marketing = Department("Marketing", users=users, threat_scores=self.generate_threat_scores(users, (30, 35)))
            finance = Department("Finance", users=users, threat_scores=self.generate_threat_scores(users, (30, 35)))
            hr = Department("HR", users=users, threat_scores=self.generate_threat_scores(users, (30, 35)))
            science = Department("Science", users=users, threat_scores=self.generate_threat_scores(users, (30, 35)))
            
            company = Company([engineering, marketing, finance, hr, science])
            aggregated_score = company.aggregatedThreatScore()

            print("All departments have similar threat scores:", aggregated_score)

            data = {
                'Department': ['Engineering', 'Marketing', 'Finance', 'HR', 'Science'],
                'Threat_Scores': [engineering.threat_scores, marketing.threat_scores, finance.threat_scores, hr.threat_scores, science.threat_scores],
                'Aggregated_Score': [aggregated_score] * 5
            }
            save_to_csv(data, filename)
        else:
            print("Reading from existing CSV file")
            print(data)

    def test_sameMeanOneHigh(self):
        filename = "test_sameMeanOneHigh.csv"
        data = read_from_csv(filename)
        
        if data is None:
            users = 10
            engineering = Department("Engineering", users=users, threat_scores=[22, 30, 25, 29, 22, 24, 26, 24, 28, 30])
            marketing = Department("Marketing", users=users, threat_scores=[23, 27, 25, 26, 27, 28, 26, 24, 29, 25])
            finance = Department("Finance", users=users, threat_scores=[20, 18, 24, 20, 17, 14, 17, 90, 18, 22])
            hr = Department("HR", users=users, threat_scores=[25, 24, 28, 26, 27, 26, 28, 25, 29, 22])
            science = Department("Science", users=users, threat_scores=[23, 28, 26, 27, 25, 28, 27, 25, 28, 23])

            company = Company([engineering, marketing, finance, hr, science])
            aggregated_score = company.aggregatedThreatScore()

            print("All departments have the same mean, but one has a very high threat score:", aggregated_score)

            data = {
                'Department': ['Engineering', 'Marketing', 'Finance', 'HR', 'Science'],
                'Threat_Scores': [engineering.threat_scores, marketing.threat_scores, finance.threat_scores, hr.threat_scores, science.threat_scores],
                'Aggregated_Score': [aggregated_score] * 5
            }
            save_to_csv(data, filename)
        else:
            print("Reading from existing CSV file")
            print(data)

    def test_differentUsers(self):
        filename = "test_differentUsers.csv"
        data = read_from_csv(filename)
        
        if data is None:
            users_Eng = 22
            users_mark = 33
            users_finance = 44
            users_hr = 55
            users_science = 66

            engineering = Department("Engineering", users=users_Eng, threat_scores=self.generate_threat_scores(users_Eng))
            marketing = Department("Marketing", users=users_mark, threat_scores=self.generate_threat_scores(users_mark))
            finance = Department("Finance", users=users_finance, threat_scores=self.generate_threat_scores(users_finance))
            hr = Department("HR", users=users_hr, threat_scores=self.generate_threat_scores(users_hr))
            science = Department("Science", users=users_science, threat_scores=self.generate_threat_scores(users_science))

            company = Company([engineering, marketing, finance, hr, science])
            aggregated_score = company.aggregatedThreatScore()

            print("All departments have different numbers of users:", aggregated_score)

            data = {
                'Department': ['Engineering', 'Marketing', 'Finance', 'HR', 'Science'],
                'Threat_Scores': [engineering.threat_scores, marketing.threat_scores, finance.threat_scores, hr.threat_scores, science.threat_scores],
                'Aggregated_Score': [aggregated_score] * 5
            }
            save_to_csv(data, filename)
        else:
            print("Reading from existing CSV file")
            print(data)

if __name__ == "__main__":
    index_name = "company_threat_scores"
    csv_filename = "threat_scores.csv"
    
    if os.path.exists(csv_filename):
        data = read_from_csv(csv_filename)
        if data is not None:
            print("created")
            create_elasticsearch_index(index_name)
            populate_elasticsearch_index_from_csv(index_name, csv_filename)
    
    unittest.main()
