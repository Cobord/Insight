import os
from flask import Flask, flash, request, redirect, url_for
from werkzeug.utils import secure_filename

UPLOAD_FOLDER='~/uploads/'
ALLOWED_EXTENSIONS=set(['wav','txt'])
FUTURE_EXTENSIONS=set(['3gp','aa','wma','mp3','mp4','flac','aiff','au','ape','wv','m4a'])

app = Flask(__name__)
app.secret_key='92840184'

@app.route('/hello/')
def hello_world():
	return 'Hello, World'

@app.route('/user/<username>')
def show_user_profile(username):
	return 'User %s'%username

def allowed_file(filename):
	dotin=('.' in filename)
	if (not dotin):
		flash('Filename please')
		return False
	extension=filename.rsplit('.', 1)[1].lower()
	if extension in ALLOWED_EXTENSIONS:
		return True
	elif extension in FUTURE_EXTENSIONS:
		flash('Not yet supported')
		return False
	flash('Sound File please')
	return False

@app.route('/', methods=['GET', 'POST'])
def upload_file():
	if request.method == 'POST':
		# check if the post request has the file part
		if 'file' not in request.files:
			flash('No file part')
			return redirect(request.url)
		file = request.files['file']
		# if user does not select file, browser also
		# submit an empty part without filename
		if file.filename == '':
			flash('No selected file')
			return redirect(request.url)
		if file and allowed_file(file.filename):
			filename = secure_filename(file.filename)
			file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
			return redirect(url_for('uploaded_file',filename=filename))
		return redirect(request.url)
	return '''
	<!doctype html>
	<title>Upload new Sound Clip</title>
	<h1>Upload in wav format only</h1>
	<form method=post enctype=multipart/form-data>
	<input type=file name=file>
	<input type=submit value=Upload>
	</form>
	'''

#@app.route('/uploaded/')
#def uploaded_file(filename):
#	return filename
#	<!doctype html>
#	<title>Thank you for uploading</title>
#	</form>
#	'''
