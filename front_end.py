import os
from flask import Flask, flash, request, redirect, url_for
from werkzeug import secure_filename

UPLOAD_FOLDER = 'unknownWavFiles'
ALLOWED_EXTENSIONS=set(['wav'])

app=Flask(__name__)
app.config['UPLOAD_FOLDER']=UPLOAD_FOLDER

def filename_is_allowed(filename):
	has_dot='.' in filename
	return (has_dot and filename.rsplit('.',1)[1].lower() in ALLOWED_EXTENSIONS


