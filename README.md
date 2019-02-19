# WaveSim

Sensing Similar Sounds

Upload a wav file and find the most similar sound from the library.

This is determined by a locality sensitive hash on the spectrogram in order to find candidate neighbors. Then among these 
candidate neighbors a more precise determination of the cosine similarity of those spectrograms is preformed.
