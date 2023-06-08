from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def home():
  return render_template('index.html')

@app.route('/result')
def result():
  # map-reduce 검색
  
  return render_template('result.html')

if __name__ == '__main__':
  app.run(debug=True)