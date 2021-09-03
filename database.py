import ast
import os

import pandas as pd
import pandas as ps

'''返回列数：
df.shape[1]
返回行数：
df.shape[0]'''


class MyDatabase():

    # 初始化数据库，输入名字，创建文件夹，并设置工作路径
    def __init__(self, name):
        self.name = name
        try:
            os.mkdir("%s" % name)

            os.chdir("./%s" % name)

        except FileExistsError:
            print("当前文件已存在，无法创建该文件。")
            # os.chdir("F:\PycharmProjects\playwright_learn\wechat_fetch")
            os.chdir("./%s" % name)
            print(os.getcwd())


    # 添加新表，创建新的csv文件，并初始化表
    def add_table(self, table_name, *args):
        """

        :param table_name:
        :param args: 初始化的属性名称
        :return:
        """

        # print(os.getcwd())
        if not os.path.exists("./%s.csv" % table_name):
            # 创建存储表,并初始化
            with open("./%s.csv" % table_name, "w+") as f:
                # 添加一个逗号，防止文件为空报错
                f.write("index")
                f.close()
            header = []

            # 遍历传入的元素，分别添加表头
            for item in args:
                self.add_header(table_name, item)
                header.append(item)

            # 初始化id
            self.__end_index = 0
            with open("./%s.txt" % table_name, 'w') as f:
                # 初始化key为name，末尾索引为1
                f.write(str({"key":"name","end_index":"1"}))
                f.close()

            # dataframe = ps.DataFrame({"name":None, "time": None, "link": None}, index=[])
            # dataframe.to_csv("./%s.csv" % table_name, encoding='gbk')
        else:
            print("当文件已存在时，无法创建该文件。")
        pass

    # 236.txt
    # {"key":"name',"end_index":"3"}
    # 添加数据时，根据key来判断是否存在
    def __is_exist(self, table_name, new):
        with open("./%s.txt" % table_name,'r') as f:
            dic = f.read()
            # 将字符串转换为字典
            dic = ast.literal_eval(dic)
            key_name = dic["key"]
            # 选择key列的值，并保存在select中
            self.select(table_name,key_name)
            # 读取select中的key_name的值
            key_colume= ps.read_csv(r"./select.csv")[key_name]
            for item in key_colume:
                if item == new[key_name]:
                    # print("数据已存在")
                    f.close()
                    return False
                else:
                    continue
            f.close()
            return True


    def set_key(self,table_name,val):
        self.__write_txt(table_name, 'key', val)
        pass


    # 定义读取存储key和index的txt文件方法
    def __read_txt(self, table_name):
        with open("./%s.txt" % table_name, 'r') as f:
            dic = f.read()
            # 将字符串转换为字典
            dic = ast.literal_eval(dic)
            f.close()
            return dic

    # 写入txt

    def __write_txt(self, table_name, tar, val):
        # 先读再写
        dic = self.__read_txt(table_name)
        with open("./%s.txt" % table_name, 'w') as f:
            dic[tar] = val
            f.write(str(dic))
            f.close()

    # 末尾索引值，读取
    def __end_index_read(self, table_name):
        dic = self.__read_txt(table_name)
        self.__end_index = int(dic['end_index'])


    # 末尾索引值，更新
    def __end_index_upgrate(self, table_name):
        dic = self.__read_txt(table_name)
        self.__end_index=int(dic['end_index'])
        self.__write_txt(table_name, 'end_index', self.__end_index + 1)


    # 增加行，列
    # 添加新表头
    def add_header(self, table_name, *args):
        """

        :param table_name:
        :param args: 传入的属性名称
        :return:
        """
        for item in args:
            # 全部使用gbk编码，就不报decode错误
            data1 = ps.read_csv(r"./%s.csv" % table_name)
            data1[item] = None
            data1.to_csv(r"./%s.csv" % table_name, index=[])

    # 在末尾添加数据
    def add_data(self, table_name, *args):
        """

        :param table_name:
        :param args: 列表格式，可包含多个列表
        :return:
        """
        # 读取表头，组合数据
        df = ps.read_csv(r"./%s.csv" % table_name)
        columns = df.columns
        data_struc = {}

        for item in args:
            # 用表头和数据构建字典结构
            if True:
                for i in range(len(item)):
                    data_struc[columns[i + 1]] = item[i]

                # 根据key，判断数据是否已经存在
                if self.__is_exist(table_name, data_struc):
                    print(data_struc)
                    self.__end_index_read(table_name)
                    dataframe = ps.DataFrame(data_struc, index=["%d" % self.__end_index])
                    dataframe.to_csv("./%s.csv" % table_name, mode="a", header=False)
                    self.__end_index_upgrate(table_name)
                else:
                    print('already exist!')

        pass

    def delete(self, table_name, attr=None, row=None, column=None):
        """

        :param table_name:
        :param attr: 通过哪一个属性定位删除的那一行，删除行时需要设置
        :param row: 输入对应属性值
        :param column: 输入对应的列名
        :return:
        """
        # 删除行，列
        if column:
            df = ps.read_csv(r"./%s.csv" % table_name)
            df.drop([column], axis=1, inplace=True)
            df.to_csv("./%s.csv" % table_name, encoding="gbk", index=[])
        if row:
            df = ps.read_csv(r"./%s.csv" % table_name)
            # y = df[df["name"].str.contains(row)]
            index = self.get_index(table_name, attr, row)
            df.drop(index, inplace=True)
            df.to_csv("./%s.csv" % table_name, index=[])
            pass

    # 获取行索引
    def get_index(self, table_name, attr, str=None):
        """

        :param table_name:
        :param attr: 属性名
        :param str: 属性具体名字
        :return:
        """

        df = ps.read_csv(r"./%s.csv" % table_name)
        y = df[df[attr].str.contains(str)]
        return [y.index[0]]

    # SELECT * FROM 236
    # WHERE NAME = "Windows系"
    #
    def select(self, table_name, *args):
        """

        :param table_name:
        :param args: 输入属性值
        :return:
        """
        df = ps.read_csv(r"./%s.csv" % table_name)
        # 每次搜索创建一个临时搜索文件，并初始化
        with open("./select.csv", "w+") as f:
            f.write("index")
            f.close()
        data = ps.read_csv(r"./select.csv")

        if args:
            for item in args:
                y = df[item]
                # print(y)
                # 添加属性列
                data[item] = y
            data.to_csv("./select.csv", index=[])
            # print(data)
            return data
        else:
            # print(df)
            return df

        # dataframe = ps.DataFrame()
        pass

    def update(self, table_name, attr, index, data):
        """
        :param table_name:
        :param attr: 属性名
        :param index: 索引位置
        :param data: 新的数据
        :return:
        """
        # 更新某一行的某一个属性,只更新一个位置
        df = ps.read_csv(r"./%s.csv" % table_name, encoding="gbk")
        df = ps.read_csv(r"./%s.csv" % table_name)
        # print(df)
        new_data = pd.Series(data, name=attr, index=index)
        # print(new_data)
        df.update(new_data)
        df.to_csv("./%s.csv" % table_name, encoding="gbk", index=[])

        pass


if __name__ == "__main__":
    database = MyDatabase("123")
    # database.add_table("236","name","time","link")
    # database.add_header("236")
    # data1 = ['mongoDB安装教程', '2021-01-19',
    #         'http://mp.weixin.qq.com/s?__biz=MzU5MTU5NTI3MQ==&mid=2247484432&idx=1&sn=39959ffcaa5ed8cf837336394e8390fb&chksm=fe2dd678c95a5f6e5bf300c1d4cce9f411418f2adcc846dde8b7ea644a4fed8c1632710eed0a#rd']
    # data2 = ['Windows系', '2021-01-15','http://mp.weixin.qq.com/s?__biz=MzU5MTU5NTI3MQ==&mid=2247484432&idx=1&sn=39959ffcaa5ed8cf837336394e8390fb&chksm=fe2dd678c95a5f6e5bf300c1d4cce9f411418f2adcc846dde8b7ea644a4fed8c1632710eed0a#rd']
    # data2 = ['Window', '2021-01-15','http://mp.weixin.qq.com/s?__biz=MzU5MTU5NTI3MQ==&mid=2247484432&idx=1&sn=39959ffcaa5ed8cf837336394e8390fb&chksm=fe2dd678c95a5f6e5bf300c1d4cce9f411418f2adcc846dde8b7ea644a4fed8c1632710eed0a#rd']
    # # data = {"name":'Windows系', "time":' 2021-01-19', "link":'http://mp.weixin.qq.com/s?__biz=MzU5MTU5NTI3MQ==&mid=2247484432&idx=1&sn=39959ffcaa5ed8cf837336394e8390fb&chksm=fe2dd678c95a5f6e5bf300c1d4cce9f411418f2adcc846dde8b7ea644a4fed8c1632710eed0a#rd'}
    # database.add_data("236",data1,data2)
    database.set_key("236",'time')


    # # database.add_header("236","新表头01")
    # # database.delete("236",column="新表头01")
    # # database.delete("236","name",row="Windows系")
    # database.select("236","index","name")
    # print(database.is_exist("236","mongoDB安装教程"))
    # # database.update("236","name",database.get_index("236","name","Windows系"),"更新测试")
    # # database.add_data("236",data2)
    # database.delete("236","name",column="新表头01")
    # database = MyDatabase("wechat_fetch")
    # data = ['"人工智能之父"马文明斯基', ' 2016-02-19', 'http://mp.weixin.qq.com/s?__biz=MzI4NjAxNjY4Nw==&mid=402287210&idx=1&sn=ff475bb493ed2d4a92fc1a16effc3437&chksm=79ed9faa4e9a16bc5531bc1163a0c61f7f333663b5bbc589c0c2bf711d8e9fc3bfa125c0d7b7#rd']
    # data = {'name': '"人工智能之父"马文・明斯基', 'time': ' 2016-02-19', 'link': 'http://mp.weixin.qq.com/s?__biz=MzI4NjAxNjY4Nw==&mid=402287210&idx=1&sn=ff475bb493ed2d4a92fc1a16effc3437&chksm=79ed9faa4e9a16bc5531bc1163a0c61f7f333663b5bbc589c0c2bf711d8e9fc3bfa125c0d7b7#rd'}
    # database.add_data("阮一峰的网络日志1", data)
    # database.add_data("阿武的大学秘籍", data)
