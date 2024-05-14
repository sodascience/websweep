# Creating documentation files for WebSweep

In case of changes to the code or working of the WebSweep package, consult the following instructions.

## Alter documentation source files

If you need to make changes to the documentation source files (typically `.rst` files), follow these steps:

1. Navigate to the `docs/source` directory.

2. Locate the relevant `.rst` file that you need to modify, according to the changes that you have made to the code.

3. Make the necessary changes to the content of the `.rst` file. RST files have a specific markup syntax which you can read more about [here](https://sphinx-tutorial.readthedocs.io/step-1/). 

4. Save the changes to the `.rst` file.

## Generate the new documentation

Once you have made changes to the documentation source files, you need to regenerate the documentation to reflect the updates. Follow these steps to generate the new documentation:

1. Open a terminal or command prompt.

2. Navigate to the root directory of the WebSweep repository.

3. Run the following command to generate the HTML documentation using Sphinx:

   ```bash
   make html
   
Sphinx will process the .rst files and generate the HTML documentation in the docs/build/html directory.

Verify that the documentation has been generated successfully and review the changes.

## Publish the documentation on Read the Docs

After generating the updated documentation locally, you can publish it on Read the Docs to make it accessible to the public. Follow these steps to publish the documentation on Read the Docs:

1. Log in to your Read the Docs account or sign up if you haven't already.

2. Navigate to your project's dashboard on Read the Docs.

3. Click on the "Settings" or "Admin" tab for your project.

4. Scroll down to the "Documentation" section and locate the "Set up documentation" or "Edit" button.

5. Follow the instructions to connect your project repository to Read the Docs and configure the documentation settings.

6. Once configured, Read the Docs will automatically build and publish your documentation whenever changes are pushed to the repository.

Verify that the documentation is accessible on Read the Docs and review it to ensure it appears as expected.

That's it! The webSweep documentation is now updated and available for public access on Read the Docs.