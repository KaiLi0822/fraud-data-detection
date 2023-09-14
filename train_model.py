# Two Step train脚本
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import sys
from sklearn2pmml.pipeline import PMMLPipeline
from sklearn2pmml import sklearn2pmml

def print_cm(cm, labels, hide_zeroes=False, hide_diagonal=False, hide_threshold=None):
    """pretty print for confusion matrixes"""
    columnwidth = max([len(x) for x in labels]) + 4
    empty_cell = " " * columnwidth
    print("    " + empty_cell, end=' ')
    for label in labels:
        print("%{0}s".format(columnwidth) % 'pred_' + label, end=" ")
    print()

    # Print rows
    for i, label1 in enumerate(labels):
        print("    %{0}s".format(columnwidth) % 'true_' + label1, end=" ")
        for j in range(len(labels)):
            cell = "%{0}.1f".format(columnwidth) % cm[i, j]
            if hide_zeroes:
                cell = cell if float(cm[i, j]) != 0 else empty_cell
            if hide_diagonal:
                cell = cell if i != j else empty_cell
            if hide_threshold:
                cell = cell if cm[i, j] > hide_threshold else empty_cell
            if cell:
                print(cell, end=" ")
        print()

def parse():
    train_file = str(sys.argv[1])
    test_file = str(sys.argv[2])
    testres_file = str(sys.argv[3])
    pmml_file = str(sys.argv[4])
    task_id = str(sys.argv[5])
    return train_file,test_file,testres_file,pmml_file,task_id


def train_model(train_file,test_file,testres_file,pmml_file,task_id):
    train_df = pd.read_csv(train_file,delimiter="\t",names=["age_ori","degree_ori","ipdiff","hchdgap_ori","isapk"]).dropna()
    stepone_train_X = train_df[["age_ori","degree_ori","ipdiff","hchdgap_ori"]]
    stepone_train_y = train_df["isapk"]
    print('-StepOne %d samples and %d features' % (stepone_train_X.shape))
    print('-StepOne %d positive out of %d total' % (sum(stepone_train_y), len(stepone_train_y)))
    # Step One RF
    stepone_rf_model = RandomForestClassifier(n_estimators=500)
    stepone_rf_model.fit(stepone_train_X, stepone_train_y)
    stepone_y_pred_score = stepone_rf_model.predict_proba(stepone_train_X)[:, 1]
    # 获取得分为零的负样本
    negative_X = stepone_train_X[(stepone_y_pred_score == 0) & (stepone_train_y == 0)]
    negative_y = pd.Series(0,index=range(negative_X.shape[0]))
    # 获取正样本
    positive_X = stepone_train_X[(stepone_y_pred_score > 0.5) | (stepone_train_y==1)]
    positive_y = pd.Series(1,index=range(positive_X.shape[0]))
    steptwo_train_X = pd.concat([negative_X,positive_X])[["age_ori","degree_ori","ipdiff","hchdgap_ori"]]
    steptwo_train_y = pd.concat([negative_y,positive_y])
    print('-StepTwo %d samples and %d features' % (steptwo_train_X.shape))
    print('-StepTwo %d positive out of %d total' % (sum(steptwo_train_y), len(steptwo_train_y)))
    # Step Two DT
    stepone_ppl_rf_model = PMMLPipeline([("classifier", RandomForestClassifier(n_estimators=10))])
    stepone_ppl_rf_model.fit(steptwo_train_X, steptwo_train_y)
    # 保存PMML模型文件
    sklearn2pmml(stepone_ppl_rf_model, pmml_file, with_repr=True)
    ###################################
    test_df = pd.read_csv(test_file, delimiter="\t",
                           names=["user_id","age_ori", "degree_ori", "ipdiff", "hchdgap_ori", "isapk"]).dropna()
    test_X = test_df[["age_ori", "degree_ori", "ipdiff", "hchdgap_ori"]]
    test_y = test_df["isapk"]
    print('-Test %d samples and %d features' % (test_X.shape))
    print('-Test %d positive out of %d total' % (sum(test_y), len(test_y)))
    # 测试模型
    test_y_pred_proba_list = stepone_ppl_rf_model.predict_proba(test_X)
    test_y_pred_proba = [item[1] for item in test_y_pred_proba_list]
    #print_cm(confusion_matrix(test_y, test_y_pred_type), labels=['negative', 'positive'])
    # 增加模型判定callback_proba列
    test_df['callback_proba'] = test_y_pred_proba
    test_df['taskid'] = task_id
    test_df[['user_id', 'isapk', 'callback_proba','taskid']].to_csv(testres_file, header=False, index=False)


if __name__ == "__main__":
    train_file,test_file,testres_file,pmml_file,task_id = parse()
    train_model(train_file,test_file,testres_file,pmml_file,task_id)