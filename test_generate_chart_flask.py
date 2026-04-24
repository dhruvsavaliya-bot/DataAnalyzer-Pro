from app import app
import io

# Create a sample CSV in memory
csv_data = io.BytesIO(b'Category,A,B\nx,10,1\nx,20,2\ny,5,3\nz,15,4\n')
client = app.test_client()

# Post file to /analyze
resp = client.post('/analyze', data={
    'file': (csv_data, 'test.csv')
}, content_type='multipart/form-data')
print('analyze status', resp.status_code)

# Attempt to generate chart
resp2 = client.post('/generate_chart', json={'type':'bar','x':'Category','y':'A'})
print('generate_chart status:', resp2.status_code)
print('json:', resp2.get_json())
