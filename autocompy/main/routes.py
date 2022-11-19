from flask import (render_template, url_for, flash, redirect, request, abort, Blueprint)
from flask_login import current_user, login_required
from autocompy import db
from autocompy.models import User
import logging, os
from autocompy import main_webm
from autocompy.main.forms import AutoCompyForm

from autocompy import log

main = Blueprint('main', __name__)


@main.route("/", methods=['GET', 'POST'])
@main.route("/home", methods=['GET', 'POST'])
@login_required
def home():
    """
    home page url, all details collected here then process in main function and return details to home page again.
    :return: home page
    """
    form = AutoCompyForm()

    # if request is post and detail option is selected below code will execute
    if request.method == 'POST' and form.complete.data:
        print("complete detail selected")
        log.info("Inside completed_details - Index ")

        # main_webm.out_file()
        log.info(" Main_webm out files called ")
        source = form.source_field.data
        sink = form.sink_field.data
        source_path = form.source.data.strip()
        sink_path = form.sink.data.strip()
        specific_cols = [x.strip() for x in form.specific_cols.data.strip().split(",")]
        # specific_cols="PRODUCT_ID, WAREHOUSE_ID"
        log.info(f"Input details collected - {source}, {sink}, {source_path}, {sink_path}, {specific_cols}")

        print(source, sink, source_path, sink_path, specific_cols)
        # call main function and getting data to web page
        # if sources are in below list then only main module called
        if source != 'select' and sink != 'select' and (source_path and sink_path):

            print("main function called")
            log.info(f"main function started")
            # calling main function for comparison
            main_webm.main(source, sink, source_path, sink_path, specific_cols)
            log.info("main_webm.main completed")
            # print(main_webm.status)
            # output_dir = os.path.join(main_webm.output_dir, 'Output', "")
            # Output directory path
            file_locations = f'File Location : {main_webm.output_dir}'

            if os.path.exists(os.path.join(main_webm.output_dir, 'report.txt')):
                with open(os.path.join(main_webm.output_dir, "errors.txt"), 'r') as f:
                    error = f.read()

                with open(os.path.join(main_webm.output_dir, 'report.txt'), 'r') as f:
                    data = f.read()

            form.report.data = data
            form.errors.data = error
            form.source_field.data = 'select'
            form.sink_field.data = 'select'
            form.specific_cols_check.data = False

            return render_template('autocompy_main.html', file_locations=file_locations, status=main_webm.status,
                                   form=form)

        else:
            log.info("ELSE ERROR : Please select and fill all the required fields")
            flash('ERROR: Please Select and Fill all the required fields !', 'danger')
            form.report.data = ""
            form.specific_cols_check.data = False
            return render_template('autocompy_main.html', form=form)


    # if request is post and basic option is selected below code will execute
    elif request.method == 'POST' and form.basic.data:
        print("basic selected")
        log.info(f"Inside Basic Select")

        source = form.source_field.data
        sink = form.sink_field.data
        source_path = form.source.data.strip()
        sink_path = form.sink.data.strip()
        # specific_cols = [x.strip() for x in request.form.get('specific_cols').strip().split(",")]
        log.info(f"Input details collected - {source}, {sink}, {source_path}, {sink_path}")

        print(source, sink, source_path, sink_path)
        # call main function and put data to web page

        if source != 'select' and sink != 'select' and (source_path or sink_path):

            print("main detail function called")
            log.info(f"main detail function started")
            main_webm.details(source, sink, source_path, sink_path)
            log.info(f"main detail function completed")

            #file_locations = f'File Location : {main_webm.output_dir}'

            with open(os.path.join(main_webm.output_dir, "report.txt"), 'r') as f:
                data = f.read()

            with open(os.path.join(main_webm.output_dir, "errors.txt"), 'r') as f:
                error = f.read()

            form.report.data = data
            form.errors.data = error
            form.source_field.data = "select"
            form.sink_field.data = "select"

            return render_template('autocompy_main.html', form=form, status=main_webm.status)

        else:

            log.info("ELSE ERROR : Please select and fill all the required fields")
            flash('ERROR: Please Select and Fill all the required fields !', 'danger')
            form.report.data = ""
            return render_template('autocompy_main.html', form=form)

    return render_template("autocompy_main.html", form=form)


@main.route("/about")
def about():
    return render_template('about.html', title='About')
