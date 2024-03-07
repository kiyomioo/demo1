import os
import ahocorasick

class word_extract:

    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])

        # 特征词路径
        self.company_path = os.path.join(cur_dir, 'data/company.txt')
        self.title_path = os.path.join(cur_dir, 'data/title.txt')
        self.salary_path = os.path.join(cur_dir, 'data/salary.txt')
        self.education_path = os.path.join(cur_dir, 'data/education.txt')
        self.keyword_path = os.path.join(cur_dir, 'data/keyword.txt')
        self.hire_manager_path = os.path.join(cur_dir, 'data/hiring_manager.txt')

        # 加载特征词
        self.company_wds = [i.strip() for i in open(self.company_path, encoding="gbk") if i.strip()]
        self.title_wds = [i.strip() for i in open(self.title_path, encoding="gbk") if i.strip()]
        self.salary_wds = [i.strip() for i in open(self.salary_path, encoding="gbk") if i.strip()]
        self.education_wds = [i.strip() for i in open(self.education_path, encoding="gbk") if i.strip()]
        self.keywords_wds = [i.strip() for i in open(self.keyword_path, encoding="gbk") if i.strip()]
        self.hire_manager_wds = [i.strip() for i in open(self.hire_manager_path, encoding="gbk") if i.strip()]
        # 领域词
        self.field_words = set(self.company_wds + self.title_wds + self.salary_wds +
                               self.education_wds + self.keywords_wds + self.hire_manager_wds)

        # 构建领域树,可以将文本流输入自动机，自动机会在文本中寻找所有预先定义的关键词，并且能够告诉你每个关键词的出现位置。
        self.field_tree = self.build_actree(list(self.field_words))
        # 构建词典
        self.word_type_dict = self.build_word_type_dict()

        return
    def build_actree(self, wordlist):
        actree = ahocorasick.Automaton()
        for index, word in enumerate(wordlist):
            actree.add_word(word, (index, word))
        actree.make_automaton()
        return actree

    def build_word_type_dict(self):  # 构造词类型
        wd_dict = dict()
        for wd in self.field_words:  # 找到用户输入的词是什么范围的
            wd_dict[wd] = []
            if wd in self.company_wds:
                wd_dict[wd].append('company')
            if wd in self.title_wds:
                wd_dict[wd].append('title')
            if wd in self.salary_wds:
                wd_dict[wd].append('salary')
            if wd in self.education_wds:
                wd_dict[wd].append('education')
            if wd in self.keywords_wds:
                wd_dict[wd].append('keyword')
            if wd in self.hire_manager_wds:
                wd_dict[wd].append('hire_manager')
        return wd_dict
    # 分类主函数

    def extract_keywords(self, text):
        # 存储匹配到的关键词及其类型
        extracted_keywords = {}
        # 使用 Aho-Corasick 自动机进行匹配
        # iter(text) 返回自动机在文本中找到的下一个匹配项的终止索引
        # end_index 以及匹配项的元组 (insert_order, word)，word 是找到的关键词。
        for end_index, (insert_order, word) in self.field_tree.iter(text):
            keyword_type = self.word_type_dict[word]
            if word in extracted_keywords:
                extracted_keywords[word].extend(keyword_type)
            else:
                extracted_keywords[word] = keyword_type
        return extracted_keywords



