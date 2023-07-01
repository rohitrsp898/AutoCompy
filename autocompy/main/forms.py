from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, TextAreaField, SelectField, BooleanField
from wtforms.validators import DataRequired


class AutoCompyForm(FlaskForm):
    sources = [('select', 'Select'), ('s3_source', 'S3*'), ('teradata', 'Teradata*'), ('ftp', 'FTP'), ('sftp', 'SFTP'),
               ('oracle', 'Oracle*'), ('local', 'Local')]
    sinks = [('select', 'Select'), ('s3_sink', 'S3*'), ('snow', 'snow*'), ('mysql_', 'MySQL'), ('local', 'Local'),
             ('web_read', 'HTTP')]

    source_field = SelectField('Select Source', validators=[DataRequired()], choices=sources, default="select")
    sink_field = SelectField('Select Sink', validators=[DataRequired()], choices=sinks, default="select")

    source = StringField('Source Details', validators=[DataRequired()])
    sink = StringField('Sink Details', validators=[DataRequired()])

    report = TextAreaField('Report', render_kw={'readonly': True}, default="")
    errors = TextAreaField('Errors', render_kw={'readonly': True}, default="")

    specific_cols_check = BooleanField('On Specific Columns ?', default=False)
    specific_cols = StringField('Enter Specific Columns')

    complete = SubmitField('Complete Details')
    basic = SubmitField('Basic Details')
