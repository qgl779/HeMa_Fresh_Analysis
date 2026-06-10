DB_CONFIG = {
    "host": "192.168.10.144",
    "port": 5432,
    "user": "hema_admin",
    "password": "hema2024",
    "database": "hema_fresh_dw"
}

MYSQL_CONFIG = {
    "host": "192.168.10.144",
    "port": 3306,
    "user": "hema_ads",
    "password": "hema2024",
    "database": "hema_fresh_ads"
}

MYSQL_JDBC_URL = "jdbc:mysql://192.168.10.144:3306/hema_fresh_ads?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai"
MYSQL_JDBC_PROPERTIES = {
    "user": "hema_ads",
    "password": "hema2024",
    "driver": "com.mysql.cj.jdbc.Driver"
}

SPARK_CONFIG = {
    "app_name": "HemaFreshAnalysis",
    "master": "yarn",
    "spark.submit.deployMode": "client",
    "spark.driver.host": "192.168.10.128",
    "spark.hadoop.fs.defaultFS": "hdfs://192.168.10.128:9000",
    "spark.executor.instances": "3",
    "spark.executor.cores": "2",
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
}

CLUSTER_MODE = True

HIVE_CONFIG = {
    "database": "hema_fresh",
    "warehouse_dir": "/user/hive/warehouse/hema_fresh.db"
}

HDFS_BASE_PATH = "hdfs://192.168.10.128:9000/hema_fresh"
HDFS_RAW_DIR = HDFS_BASE_PATH + "/ods_raw"
HDFS_DWD_DIR = HDFS_BASE_PATH + "/dwd"
HDFS_DWS_DIR = HDFS_BASE_PATH + "/dws"
HDFS_FEATURES_DIR = HDFS_BASE_PATH + "/features"

PG_JDBC_URL = "jdbc:postgresql://192.168.10.144:5432/hema_fresh_dw"
PG_JDBC_PROPERTIES = {
    "user": "hema_admin",
    "password": "hema2024",
    "driver": "org.postgresql.Driver"
}

CITIES = ["上海", "北京", "深圳", "广州", "杭州", "成都", "武汉", "南京", "苏州", "西安"]

STORES = [
    "金桥店", "长宁来福士店", "大宁店", "虹桥店", "静安店",
    "十里堡店", "朝阳大悦城店", "海淀万柳店", "亦庄店",
    "南山店", "福田店", "龙华店",
    "天河店", "海珠店", "番禺店",
    "西湖银泰店", "滨江店", "城西银泰店",
    "锦江店", "武侯店",
    "武昌店", "光谷店",
    "新街口店", "鼓楼店",
    "园区店", "昆山店",
    "雁塔店"
]

CATEGORIES = {
    "水果": ["苹果", "香蕉", "车厘子", "蓝莓", "草莓", "芒果", "猕猴桃", "榴莲", "橙子", "葡萄", "西瓜", "桃子", "柠檬", "牛油果", "椰子"],
    "蔬菜": ["西红柿", "黄瓜", "生菜", "菠菜", "西兰花", "胡萝卜", "土豆", "茄子", "青椒", "蘑菇", "玉米", "芹菜", "豆芽", "南瓜", "洋葱"],
    "肉禽蛋": ["鸡胸肉", "五花肉", "牛腱子", "羊排", "鸡蛋", "鸭蛋", "鸡翅", "猪排骨", "牛肉卷", "鸡腿", "培根", "腊肉", "乌骨鸡", "鹌鹑蛋", "牛腩"],
    "海鲜水产": ["三文鱼", "基围虾", "大闸蟹", "鲍鱼", "扇贝", "带鱼", "鲈鱼", "小龙虾", "生蚝", "鳕鱼", "多宝鱼", "皮皮虾", "海参", "花蛤", "帝王蟹"],
    "乳制品": ["鲜牛奶", "酸奶", "奶酪", "黄油", "淡奶油", "芝士片", "冰淇淋", "炼乳", "奶油奶酪", "酸奶油"],
    "烘焙": ["吐司面包", "法棍", "牛角包", "蛋糕卷", "蛋挞", "瑞士卷", "麻薯", "可颂", "司康", "披萨饼底"],
    "熟食": ["烤鸡", "卤牛肉", "寿司拼盘", "沙拉", "麻辣小龙虾", "烤鸭", "炸鸡", "凉皮", "肉夹馍", "三明治"],
    "冷冻食品": ["速冻水饺", "汤圆", "冰淇淋", "冷冻披萨", "速冻馄饨", "关东煮", "冷冻薯条", "火锅丸子", "冷冻蔬菜", "冷冻海鲜"],
    "粮油调味": ["橄榄油", "生抽", "蚝油", "盐", "白糖", "料酒", "豆瓣酱", "花椒", "大米", "面粉"],
    "零食饮料": ["薯片", "坚果", "果汁", "可乐", "矿泉水", "咖啡", "巧克力", "饼干", "茶饮料", "能量棒"]
}

CUSTOMER_TAGS = ["高价值", "家庭用户", "健身达人", "上班族", "宝妈", "学生党", "美食爱好者", "价格敏感型"]

WEEKDAY_EFFECT = {0: 0.85, 1: 0.90, 2: 0.88, 3: 0.92, 4: 0.95, 5: 1.20, 6: 1.25}

SEASON_EFFECT = {1: 1.10, 2: 0.95, 3: 1.05, 4: 0.90, 5: 0.92, 6: 1.00,
                 7: 1.15, 8: 1.18, 9: 1.05, 10: 0.95, 11: 1.10, 12: 1.30}

MEMBERSHIP_LEVELS = ["普通会员", "黄金会员", "钻石会员", "X会员"]

PRODUCT_BASE_PRICES = {}

for cat, products in CATEGORIES.items():
    if cat in ("海鲜水产", "肉禽蛋"):
        low, high = 20, 200
    elif cat in ("水果", "乳制品"):
        low, high = 10, 80
    elif cat in ("熟食", "烘焙"):
        low, high = 15, 60
    elif cat in ("冷冻食品", "零食饮料"):
        low, high = 5, 50
    else:
        low, high = 3, 40
    for p in products:
        import random
        PRODUCT_BASE_PRICES[p] = round(random.uniform(low, high), 2)

SHELF_LIFE_DAYS = {}
for cat, products in CATEGORIES.items():
    if cat in ("蔬菜", "水果"):
        days = [3, 5, 7]
    elif cat in ("肉禽蛋", "海鲜水产", "熟食"):
        days = [1, 2, 3, 5]
    elif cat in ("乳制品", "烘焙"):
        days = [5, 7, 10, 14]
    else:
        days = [30, 60, 90, 180]
    for p in products:
        import random
        SHELF_LIFE_DAYS[p] = random.choice(days)
