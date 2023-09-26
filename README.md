## 1. Giới thiệu và cài đặt Spark
## 2. Spark RDD
## 3. SparkSQL, Dataframes và Datasets
## 4. SparkSQL và SparkSQL Table
## 5. Data Transformation với Spark
## 6. Data Aggregations và Join trên Spark
## 7. Spark Streaming
## 8. Đọc dữ liệu với Kafka Source và các phép Join với Stream
### 8.1. Join 1 streaning DF với 1 DF tĩnh
### 8.2. Join 1 streaming DF vs streaming DF
### 8.3. Streaming Watermark
### 8.4. Streaming outer joins
## 9. Streaming Windowing và Aggregates
### 9.1 Stateless transformation và statefull transformation
### 9.2 Tumbling window và sliding window
## 10. 1 số thông tin
- config("spark.streaming.stopGracefullyOnShutdown", "true") để khi ứng dụng tắt vì lý do bảo trì hay lỗi thì nó duy trì các đặc điểm Exactly Once. Exactly Once có 2 điều:
  - Không bỏ xót bất kì bản ghi đầu vào nào
  - Không tạo bản ghi đầu ra trùng lặp
- config("spark.sql.streaming.schemaInference", "true"): bình thường thì spark sẽ disable schemaInference
- option('maxFilesPerTrigger', '1'): chỉ định chỉ đọc 1 file mỗi micro-batch
- outputMode('append'): mode là append thì sẽ chỉ ghi các bản ghi mới. Streamwriter cho phép chỉ định 3 loại outputMode là :
  - append: chỉ ghi các bản ghi mới
  - update: chỉ thấy các bản ghi mới hoặc các giá trị cũ được cập nhật
  - complete: cơ chế này in ra hoàn chỉnh kết quả 
- option("checkpointLocation", "chk-point-dir") : chk-point-dir là thư mục lưu thông tin tiến trình của các micro-batch, trong này chủ yếu chưa 2 thứ: read position (vị trí đọc) và state information (trạng thái thông tin). Thông tin vị trí đọc thể hiện điểm bắt đầu và kết thúc dang xử lý bởi micro-batch, sau khi batch kết thúc, spark cũng cam kết rằng vùng dữ liệu đã được xử lý thành công. State information là lưu dữ liệu trung gian cho micro-batch, ví dụ khi cần tính 1 thông số nào đó được tổng hợp từ các batch thì sẽ cần lưu giữ các dữ liệu trung gian này để tính ra kết quả cuối cùng.
- trigger(processingTime='1 minute'): thời gian cho mỗi micro-batch xử lý là 1 phút 
