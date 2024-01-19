import xgboost as xgb
from sklearn.datasets import load_boston
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt

# 加载数据集
boston = load_boston()
X, y = boston.data, boston.target

boston = load_boston()
X,y = boston.data,boston.target
# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
X_train,X_test,y_train,y_test = train_test_split(X,y,test_size=0.2,random_state=42)
# 归一化处理
mean = X_train.mean(axis=0)
mean = X_train.mean(axis=0)

std = X_train.std(axis=0)
std = X_train.std(axis=0)
X_train = (X_train - mean) / std
X_train = (X_train-mean) /std
X_test = (X_test - mean) / std
X_test = (X_test-mean) /std

# 将数据集转换为特征矩阵
dtrain = xgb.DMatrix(X_train, label=y_train)
dtest = xgb.DMatrix(X_test, label=y_test)
dtrain = xgb.DMatrix(X_train,label=y_train)
dtest = xgb.DMatrix(X_test,label=y_test)

# 设置XGBoost参数
params = {
    'booster': 'gbtree',
    'objective': 'reg:squarederror',
    'eval_metric': 'rmse',
    'eta': 0.1,
    'max_depth': 3,
    'subsample': 0.8,
    'colsample_bytree': 0.8
}

params ={
    'booster': 'gbtree',
    'objective':'reg:squarederror',
    'eval_metric': 'rmse',
    'eta':0.1,
    'max_depth':3,
    'subsample':0.8,
    'colsample_bytree':0.8
}
# 训练模型
model = xgb.train(params, dtrain, num_boost_round=100)
model = xgb.train(params,dtrain,num_boost_round=100)
# 预测结果
y_pred = model.predict(dtest)
y_pred = model.predict(dtest)
# 计算均方误差
mse = mean_squared_error(y_test, y_pred)
mse = mean_squared_error(y_test,y_pred)
print("Mean Squared Error:", mse)

# 可视化特征重要性
xgb.plot_importance(model)
xgb.plot_importance(model)
plt.show()
plt.show()