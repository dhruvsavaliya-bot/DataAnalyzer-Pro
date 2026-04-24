import requests
s = requests.Session()
with open('test_sample.csv','w') as f:
    f.write('Category,A,B\n x,10,1\n x,20,2\n y,5,3\n z,15,4\n')
print('Uploading file...')
r = s.post('http://127.0.0.1:5000/analyze', files={'file': open('test_sample.csv','rb')})
print('analyze status', r.status_code)
print('analyze response snippet:', r.text[:200])

print('Requesting chart...')
payload = {'type':'bar','x':'Category','y':'A'}
r2 = s.post('http://127.0.0.1:5000/generate_chart', json=payload)
print('generate status', r2.status_code)
print('generate response headers:', r2.headers.get('Content-Type'))
print('generate response:', r2.text[:500])
