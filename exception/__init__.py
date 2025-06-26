import os
import sys

def error_message_detail(error, error_detail:sys):
    _, _, exc_tb = error_detail.exc_info()  # exc_tb -> traceback object
    file_name = exc_tb.tb_frame.f_code.co_filename
    error_message = "Error occurred python script name [{0}] line number [{1}] error message [{2}]".format(
        file_name, exc_tb.tb_lineno, str(error)
    )

    return error_message


class ETL_Exception(Exception):
    def __init__(self, error, error_detail: sys):

        self.error_message = error_message_detail(error, error_detail)
        super().__init__(self.error_message)

    def __str__(self):
        return self.error_message