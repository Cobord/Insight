import os
from flask import Flask, flash, request, redirect, url_for
from werkzeug.utils import secure_filename
import wav_to_hash3
import save_elastic_search2

UPLOAD_FOLDER='./uploads/'
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
			extension=filename.rsplit('.', 1)[1].lower()
			save_loc=os.path.join(UPLOAD_FOLDER, 'temp.'+extension)
			file.save(save_loc)
			return redirect(url_for('uploadedFile',filename=filename))
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

@app.route('/uploadedFile/<filename>')
def uploadedFile(filename):
	extension=filename.rsplit('.',1)[1].lower()
	save_loc=os.path.join(UPLOAD_FOLDER,'temp.'+extension)
	(my_hashes,my_spec)=wav_to_hash3.lsh_and_spectra_of_unknown(save_loc,'all_hp.npy')
	potentials=save_elasticsearch2.get_any_matches(my_hashes[0],my_hashes[1],my_hashes[2],my_hashes[3],my_hashes[4])
	scored_cands=wav_to_hash3.score_false_positives2(potentials,my_spec)
	return str(stored_cands)
	<!doctype html>
	<title>Uploaded File</title>
	<h1>Just showing random from the library instead</h1>
	'''
