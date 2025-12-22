# batch-ml：离线 ETL、训练与推理服务

面向高速公路 ETC 的离线数据处理与简单模型训练/推理仓库，目标是：

1) 用 Spark 进行批量清洗、特征加工、统计；2) 训练模型并导出 ONNX；3) 通过 FastAPI + ONNXRuntime 对外提供推理服务。

## 技术栈

- Spark (ETL/统计)；Python 3.10+
- 训练/推理：ONNXRuntime + NumPy + FastAPI + Uvicorn
- 持续集成：.github/workflows/python-ci.yml（lint、单测、可选镜像构建）

## 目录结构

- jobs/：示例或实际 Spark 作业脚本（批量清洗、统计、特征）
- models/：训练脚本、特征配置与模型导出（ONNX）
- serving/：推理服务，入口 `app:app`，依赖见 serving/requirements.txt
- data/（自建）：可放中间数据或样例数据，避免提交大文件

## 快速上手

### 环境准备

- Python 3.10+，pip；可选：Spark 本地 standalone 或远程集群
- 安装推理依赖：`cd serving && pip install -r requirements.txt`

### 运行推理服务（本地）

```bash
cd serving
uvicorn app:app --reload --port 8001
```

默认暴露 POST /predict（具体请求体以 app.py 中的 pydantic 模型为准），ONNX 模型文件路径可在环境变量或配置中修改。

### 运行 Spark 作业（示例）

```bash
spark-submit --master local[*] jobs/example_job.py \
 --input hdfs:///data/traffic_raw \
 --output hdfs:///data/traffic_features
```

请替换 input/output 与具体脚本；如在 Yarn/K8s 提交，补充对应 master 与资源参数。

## 典型流程

1) 准备原始 CSV/Parquet → jobs/ 中的清洗脚本输出干净数据/特征。
2) 在 models/ 中训练并导出 ONNX，记录版本号与输入特征顺序。
3) 将模型文件放入 serving/ 或挂载路径，启动 FastAPI 服务，对外提供推理。
4) 产出结果可供 streaming/ 或 services/ 读取，需在文件名/存储路径上约定清晰。

## 配置与约定

- ONNXRuntime 版本以 serving/requirements.txt 为准；若需 GPU，请根据硬件更换对应包。
- 推理服务可通过环境变量指定模型路径、并发/批大小；建议在 README 记录自定义项。
- Spark 作业的输入输出路径、分区数、checkpoint 目录等参数请在提交时写在脚本头部注释。

## 开发规范

- 代码风格：遵循 PEP8，使用 Ruff/Black（可在 CI 中开启）；尽量保持类型标注。
- 日志：Spark 作业使用 log4j 配置；FastAPI 使用标准 logging，避免打印敏感数据。
- 数据：不提交大体积或敏感数据，示例数据可裁剪后放入 data/sample。

## 常见问题

- ONNXRuntime 报错 “CPUExecutionProvider not found”：确认安装 `onnxruntime` 而非 `onnxruntime-gpu`，并匹配 Python 版本。
- Spark 提交卡住：检查 master 地址、资源参数以及输入路径是否可读；本地测试可先用 `--master local[*]`。
- 模型维度不匹配：确认训练脚本与推理端的特征顺序一致，必要时在模型旁放置 schema 说明。
