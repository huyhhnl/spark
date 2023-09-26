- [Questions.csv datataset](https://drive.google.com/file/d/1yH2lTY3mGOoGEDW1Y2lhzRwDuaZvI1KI/view?usp=sharing)
- [Answers.csv dataset](https://drive.google.com/file/d/1gEU9KojKI3yMybjL0_tYsQnzRlUoBYro/view?usp=sharing)
## 1. Questions.csv

File csv chứa các thông tin liên quan đến câu hỏi của hệ thống, với cấu trúc như sau:

- Id: Id của câu hỏi.

- OwnerUserId: Id của người tạo câu hỏi đó. (Nếu giá trị là NA thì tức là không có giá trị này).

- CreationDate: Ngày câu hỏi được tạo.

- ClosedDate: Ngày câu hỏi kết thúc (Nếu giá trị là NA thì tức là không có giá trị này).

- Score: Điểm số mà người tạo nhận được từ câu hỏi này.

- Title: Tiêu đề của câu hỏi.

- Body: Nội dung câu hỏi.

## 2. File Answers.csv

File csv chứa các thông tin liên quan đến câu trả lời và có cấu trúc như sau:

- Id: Id của câu trả lời.

- OwnerUserId: Id của người tạo câu trả lời đó. (Nếu giá trị là NA thì tức là không có giá trị này)

- CreationDate: Ngày câu trả lờiđược tạo.

- ParentId: ID của câu hỏi mà có câu trả lời này.

- Score: Điểm số mà người trả lờinhận được từ câu trả lời này.

- Body: Nội dung câu trả lời.
## 3. Đưa dữ liệu vào MongoDB
- mongoimport --type csv -d <database> -c <collection> --headerline --drop <file>
## 4.  Đọc dữ liệu từ MongoDB với Spark
- Lưu ý về cách đọc ghi giữa các phiên bản mongo
