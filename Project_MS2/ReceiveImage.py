import redis        # pip install redis
import io;
import base64

ip=""
r = redis.Redis(host=ip, port=6379, db=0,password='sofe4630u')

value=r.get('A_001');
decoded_value=base64.b64decode(value);

with open("./receivedTest.jpg", "wb") as f:
    f.write(decoded_value);
    
print('Image received, check ./receivedTest.jpg')
