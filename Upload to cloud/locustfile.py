from locust import HttpUser, task, between

class FileUploadTest(HttpUser):
    wait_time = between(1, 2)  # Simulate user wait time

    @task
    def upload_file(self):
        with open("test1.pdf", "rb") as f:
            files = {"file": ("test1.pdf", f, "application/pdf")}
            response = self.client.post("/upload/", files=files)
            print(response.json())

