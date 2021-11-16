# Change as needed
python_ver='python3.9'


# Keep this first so 'make' works
# Install Package
install:
	$(python_ver) setup.py install

# Clean up after build
clean:
	rm -r build dist messari.egg-info

# Uninstall package
uninstall:
	$(python_ver) -m pip uninstall messari

# List options
list:
	@grep '^[^#[:space:]].*:' Makefile
