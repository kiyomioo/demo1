import csv
import os
import Levenshtein
from fuzzywuzzy import fuzz
from py2neo import Node, Graph


class build_kg:
    def __init__(self):
        self.graph = Graph("http://localhost:7474", user="neo4j", password="123456")
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.data_path=os.path.join(cur_dir,'data/data1.csv')
        self.node_dict = {}  # 记录重复节点
    def read_nodes(self):
        #构建实体节点
        company=[]  # 公司
        title=[]   # 职位
        salary=[]    # 工资
        education=[]     # 学历
        keyword =[]   # for every keyword in description
        hiring_manager=[] # 招聘者
        link=[] # 链接

        company_infos=[]    # 公司信息,公司有地址属性

        #构建节点实体关系
        rel_title_des=[]   # 职位_要求关系
        rel_company_des=[]  # 公司_要求关系
        rel_title_sa=[]   #  职位_薪资关系
        rel_company_sa =[]   #  公司_薪资关系
        rel_title_edu=[]
        rel_company_edu = []
        rel_title_link = []
        rel_company_link = []
        rel_title_hire = []
        rel_company_hire = []
        rel_company_title = []

        with open(self.data_path, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                company_dict = {}  # 存储company所有属性
                company_name = row['company']   # company名字
                company_dict['company'] = company_name
                company.append(company_name)
                company_dict['address'] = ''

                if 'title' in row and 'company' in row:
                    title.append(row['title'])
                    rel_company_title.append([company_name,row['title']])

                if 'salary' in row and 'company' in row and 'title' in row:
                    salary.append(row['salary'])
                    rel_company_sa.append([company_name,row['salary']])
                    rel_title_sa.append([row['title'], row['salary']])

                if 'education' in row and 'company' in row and 'title' in row:
                    education.append(row['education'])
                    rel_company_edu.append([company_name, row['education']])
                    rel_title_edu.append([row['title'], row['education']])

                if 'description' in row and 'company' in row and 'title' in row:
                    keywords = row['description'].split('、')
                    for kw in keywords:
                        rel_company_des.append([company_name, kw])
                        rel_title_des.append([row['title'], kw])
                        keyword.extend(keywords)

                if 'hiring_manager'in row and 'company' in row and 'title' in row:
                    hiring_manager.append(row['hiring_manager'])
                    rel_company_hire.append([company_name, row['hiring_manager']])
                    rel_title_hire.append([row['title'], row['hiring_manager']])

                if 'link' in row and 'company' in row and 'title' in row:
                    link.append(row['link'])
                    rel_company_link.append([company_name,row['link']])
                    rel_title_link.append([row['title'],row['link']])

                if 'address' in row:
                    company_dict['address'] = row['address']
                company_infos.append(company_dict)

        return set(company), set(title), set(salary), set(education),set(keyword),set(hiring_manager),\
            set(link),company_infos, rel_company_sa, rel_company_edu, rel_company_hire,rel_company_link,\
            rel_company_des,rel_title_sa,rel_title_edu,rel_title_hire,rel_title_link,rel_title_des,rel_company_title

    # 创建普通节点
    def create_node(self,label,nodes):
        """
        :param label: 节点标签(即名称)
        :param nodes: 具体节点
        :return: None
        """
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.graph.create(node)
            self.node_dict[node_name] = node
        return self.node_dict

    # 创建 company 节点
    def create_company_nodes(self,company_infos):
        """
        :param company_infos: 公司节点具体信息
        :return: None
        """
        for company_dict in company_infos:
            node=Node("Company",name=company_dict['company'],address=company_dict['address'])
            self.graph.create(node)

        return

    # 创建图谱的实体节点
    def create_graph_nodes(self):
        self.graph.delete_all()
        company, title, salary,education,keyword,hiring_manager,link, company_infos, \
            rel_company_sa, rel_company_edu, rel_company_hire,rel_company_link,\
            rel_company_des, rel_title_sa, rel_title_edu,rel_title_hire,rel_title_link,rel_title_des,\
            rel_company_title = self.read_nodes()
        print('创建公司节点...')
        self.create_company_nodes(company_infos)
        print('创建职位节点...')
        self.create_node('Title',title)
        print('创建薪资节点...')
        self.create_node('Salary',salary)
        print('创建学历节点...')
        self.create_node('Education',education)
        print('创建要求节点...')
        self.create_node('Keyword', keyword)
        print('创建招聘者节点...')
        self.create_node('Manager', hiring_manager)
        print('创建链接节点...')
        self.create_node('Link', link)
        return

    # 创建关系
    def create_relationship(self,start_node,end_node,edges,rel_type,rel_name):
        """
        :param start_node: 起始节点
        :param end_node: 终点节点
        :param edges: 边
        :param rel_type: 关系类型
        :param rel_name: 关系名字
        :return: None
        """
        #去重处理
        set_edges = []
        for edge in edges:
            set_edges.append('|'.join(edge))  # 使用|作为不同关系之间分隔的标志
        for edge in set(set_edges):
            edge = edge.split('|')
            p = edge[0]
            q = edge[1]

            query = "MATCH (p:%s), (q:%s) WHERE p.name = '%s' AND q.name = '%s' CREATE (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node,end_node,p,q,rel_type,rel_name)
            # match语法，p，q分别为标签，rel_type表示关系类别，rel_name 关系名字
            try:
                self.graph.run(query)
            except Exception as e:
                print(e)
        return

    # 创建图谱关系
    def create_graph_rels(self):
        company, title, salary, education, keyword, hiring_manager, link, company_infos, \
            rel_company_sa, rel_company_edu, rel_company_hire, rel_company_link, \
            rel_company_des, rel_title_sa, rel_title_edu, rel_title_hire, rel_title_link, rel_title_des,\
            rel_company_title = self.read_nodes()
        print('创建公司与职位的联系')
        self.create_relationship('Company', 'Title', rel_company_title, 'offer', '提供工作')

        print('创建公司关系...')
        self.create_relationship('Company','Salary',rel_company_sa,'pay','付薪水')
        self.create_relationship('Company','Education',rel_company_edu,'require_edu','要求学历')
        self.create_relationship('Company', 'Keyword', rel_company_des, 'require_ability', '要求能力')
        self.create_relationship('Company','Manager',rel_company_hire,'hire','负责招聘')
        self.create_relationship('Company','Link',rel_company_link,'detail','查看详情')

        print('创建职位关系...')
        self.create_relationship('Title', 'Salary', rel_title_sa, 'pay', '付薪水')
        self.create_relationship('Title', 'Education', rel_title_edu, 'require_edu', '要求学历')
        self.create_relationship('Title', 'Keyword', rel_title_des, 'require_ability', '要求能力')
        self.create_relationship('Title', 'Manager', rel_title_hire, 'hire', '负责招聘')
        self.create_relationship('Title', 'Link', rel_title_link, 'detail', '查看详情')

    def export_data(self):
            company, title, salary, education, keyword, hiring_manager, link, company_infos, \
                rel_company_sa, rel_company_edu, rel_company_hire, rel_company_link, \
                rel_company_des, rel_title_sa, rel_title_edu, rel_title_hire, rel_title_link, rel_title_des, \
                rel_company_title = self.read_nodes()
            f_company = open('data/company.txt', 'w+')
            f_title = open('data/title.txt', 'w+')
            f_salary = open('data/salary.txt', 'w+')
            f_education = open('data/education.txt', 'w+')
            f_keyword = open('data/keyword.txt', 'w+')
            f_hiring_manager = open('data/hiring_manager.txt', 'w+')

            f_company.write('\n'.join(list(company)))
            f_title.write('\n'.join(list(title)))
            f_salary.write('\n'.join(list(salary)))
            f_education.write('\n'.join(list(education)))
            f_keyword.write('\n'.join(list(keyword)))
            f_hiring_manager.write('\n'.join(list(hiring_manager)))

            f_company.close()
            f_title.close()
            f_salary.close()
            f_education.close()
            f_keyword.close()
            f_hiring_manager.close()
            return





