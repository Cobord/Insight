# WaveSim

# Introduction

Sensing Similar Sounds

FreeSounds allows people to upload data sets of sound they recorded as packs. This provides a large library of freely available sounds. In order to avoid redunancy, the users need to be able to identify similar sounds already in the library.

# Architecture

Upload a wav file and find the most similar sound from the library.

This is determined by a locality sensitive hash on the spectrogram in order to find candidate neighbors. Then among these 
candidate neighbors a more precise determination of the cosine similarity of those spectrograms is preformed.

The computation of spectrograms and locality 
