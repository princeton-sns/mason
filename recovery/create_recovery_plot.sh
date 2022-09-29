echo "Deleting old seqnum files..."
rm -f *.receivedSequenceNumbers

echo "ls-ing so we know we deleted everything..."
ls
echo

echo "Moving seqnum files..."
mv -f ../client/*.receivedSequenceNumbers .

echo "Generating datfile..."
python3 generate_recovery_datfile.py 4 # assumes 4 sequence spaces as in the paper

echo "Plotting recovery..."
gnuplot plot_recovery.gnu
