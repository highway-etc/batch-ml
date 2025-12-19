# batch-ml (Spark & ML)

- jobs/: Spark ETL/统计
- models/: 训练脚本与模型导出（ONNX）
- serving/: 模型推理服务（FastAPI/ONNXRuntime）
- .github/workflows/python-ci.yml: lint/test/构建推送

## 本地开发

- Spark 作业：根据实际环境准备 Spark 集群或本地 standalone，示例提交：

```bash
spark-submit --master local[*] jobs/example_job.py
```

- 推理服务：

```bash
cd serving
pip install -r requirements.txt
uvicorn app:app --reload --port 8001
```

## 依赖与约定

- Python 3.10+ 建议，ONNXRuntime 版本以 `serving/requirements.txt` 为准。
- 离线产出的统计/模型可被 streaming/services 使用，目录约定请在提交前在 README 中注明。
- CI 会运行 lint/test 并可选择构建镜像，保持 requirements 与代码同步。
