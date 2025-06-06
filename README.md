## FORECASTING BUSINESS PERFORMANCE

### Một số vấn đề đã gặp phải khi thực hiện dự án:

Kết nối pyspark để đẩy dữ liệu vào snowflake: gặp lỗi JDBC driver encountered communication error. Message: Exception encountered for HTTP request: sun.security.validator.ValidatorException: No trusted certificate found.

Cách khắc phục: thêm tham số "insecureMode": "true" vào sfOptions, nhưng cách này không thực sự tốt nếu triển khai ở môi trường productive.