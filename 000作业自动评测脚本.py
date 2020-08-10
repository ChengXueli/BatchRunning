import logging
import multiprocessing as mp
import os
import platform
import re
import signal
import smtplib
import subprocess
import sys
import time
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from timeit import default_timer as time_counter

import numpy as np
import pdfkit
from apscheduler.schedulers.blocking import BlockingScheduler
from matplotlib import pyplot as plt
from prettytable import PrettyTable, ALL

# 需要安装wkhtmltopdf
# 在ubuntu上使用：sudo apt install wkhtmltopdf
# 在windows上手动下载：https://wkhtmltopdf.org/downloads.html, 放在当前目录或者环境变量中

Job_ID = None

Input_File = "input.txt"
# 是否使用多进程
Multi_Process = True
Is_QQ = False

# 定时任务
Scheduled = False
Hour, Minute, Second = 20, 10, 0
# 截止日期
DDL = "2020-08-30 23:59:59"

# 超时时间 5 s
Timeout = 5
Timeout_TEXT = "timeout"
Unknown_TEXT = "unknown"

# 邮箱配置相关
Server_Addr = "mail.std.uestc.edu.cn"
# 当前管理者学号，用于确定学生邮箱账号
Admin = ""
# 邮箱密码，需要到校园邮箱生成。
Password = ""

Date_Format0 = "%Y_%m_%d_%H_%M_%S"
Date_Format = "%Y/%m/%d %H:%M:%S"
exe_pat = r"\S+?_(.+?)[_|\.]\S*exe"

plt.rcParams['font.sans-serif'] = ['SimHei']  # 显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 这两行需要手动设置


class DirectoryHelper(object):
    log_dir = os.path.curdir

    @staticmethod
    def set_log_dir(job_id, ts):
        DirectoryHelper.log_dir = "_".join([str(job_id), str(ts)])

    @staticmethod
    def get_log_name(filename):
        return os.path.join(DirectoryHelper.log_dir, filename)


class EmailSender(object):
    def __init__(self, server, user, passwd):
        self.a = None
        self.server = smtplib.SMTP(server)
        self.from_addr = user if re.match(r"\S+@\S+", user) else EmailSender.get_email_addr(user)
        self.password = passwd
        self.login = False

    @staticmethod
    def get_email_addr(student_id, is_qq=False):
        if is_qq:
            return str(student_id) + "@qq.com"
        return str(student_id) + "@std.uestc.edu.cn"

    def send(self, to, subject, text, attachment=None, is_qq=False, html=False):
        if not self.login:
            self.server.login(self.from_addr, self.password)
            self.login = True

        if not re.match(r"\S+@\S+", to):
            to = EmailSender.get_email_addr(to, is_qq=is_qq)
        if html:
            message = MIMEText(text, "html", "utf-8")
        else:
            message = MIMEText(text, "plain", "utf-8")
        message["Subject"] = Header(subject, "utf-8")
        message["From"] = self.from_addr
        message["To"] = to
        if attachment is not None:
            text_message = message
            message = MIMEMultipart()
            message["Subject"] = Header(subject, "utf-8")
            message["From"] = self.from_addr
            message["To"] = to
            attach_message = MIMEApplication(open(attachment, "rb").read())
            attach_message.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
            message.attach(text_message)
            message.attach(attach_message)
        self.server.sendmail(self.from_addr, to, message.as_string())
        return self

    def close(self):
        self.server.close()
        self.login = False

    def quit(self):
        self.server.quit()
        del self.from_addr
        del self.password
        self.login = False


class Timer(object):
    def __init__(self):
        self._start_time = 0
        self._end_time = 0
        self._record = []

    def start(self):
        self._start_time = time_counter()

    def tick(self, reset=True, unit_us=True):
        self._end_time = time_counter()
        if unit_us:
            diff = int((self._end_time - self._start_time) * 1e3)
        else:
            diff = int((self._end_time - self._start_time) * 1e3) / 1000
        self._record.append(diff)
        if reset:
            self.reset()
        return diff

    def reset(self):
        self._record.clear()

    def get_records(self):
        return self._record


class DataHandler(object):
    @staticmethod
    def html2pdf(text, filename):
        if os.path.exists(filename):
            os.remove(filename)
        pdfkit.from_string(text, filename, options={"--quiet": ""})

    @staticmethod
    def construct_html_email(tab):
        html = """<html>
        <head><meta charset="UTF-8"><style>div{font-family: 'Consolas',serif;font-size: 20px;}</style></head>
        <body><div>%s</div></body></html>""" % str(tab).replace("\n", "<br/>").replace(" ", "&nbsp;")
        return html

    @staticmethod
    def construct_html_pdf(data_matrix, true_false, title):
        html = """<html><head><meta charset="UTF-8"><style>
                    td{
                        min-width: 50px;
                    }
                    .col_header{
                        min-width: 100px;
                        font-weight: bold;
                    }
                    .error_cls{
                        color: red;
                    }
                    .ok_cls{
                        color: green;
                    }
                    .inputs {
                        font-weight: bold;
                    }
                    </style></head><body><div><table align="center" border="1" cellspacing="0px" style="border-collapse:collapse; min-width: 50%; font-size: x-large">"""
        html += '<caption style="text-align:left">%s</caption>' % title.replace("\t", "".join(["&nbsp"] * 4))
        for row_index, row_data in enumerate(data_matrix):
            html += '<tr>'
            for case, data in enumerate(row_data):
                cls = "normal"
                if row_index == 0:
                    cls = "inputs"
                if case == 0:
                    cls = "col_header"
                if row_index == 1 and case > 0:
                    if true_false[case - 1]:
                        cls = "ok_cls"
                    else:
                        cls = "error_cls"

                html += '<td class="%s">' % cls
                html += str(data)
                html += '</td>'
            html += '</tr>'
        html += '</table></div></body></html>'
        return html

    @staticmethod
    def construct_pretty_table(rows_header, cols_header, student_results, baseline_list):
        tab = PrettyTable(header=True, vrules=ALL, hrules=ALL)
        # 添加第一列的列头
        tab.add_column(fieldname=rows_header[0], column=rows_header[1:])
        for col in range(len(cols_header)):
            data = list()
            data.extend(student_results[col][1:])
            if baseline_list is not None and len(baseline_list) > 0:
                for baseline in baseline_list:
                    data.extend(baseline[1][col][1:])
            tab.add_column(fieldname=cols_header[col], column=data)
        return tab

    @staticmethod
    def construct_headers(student_results, baseline_list):
        rows_header = list(["Input", "Output", "Time(ms)"])
        if baseline_list is not None and len(baseline_list) > 0:
            if len(baseline_list) == 1:
                rows_header.append("B/L Output")
                rows_header.append("B/L Time")
            else:
                for i in range(len(baseline_list)):
                    rows_header.append("B/L Output" + str(i))
                    rows_header.append("B/L Time" + str(i))

        cols_header = list()
        true_false = list()
        if student_results is not None and len(student_results) > 0:
            for ans, res in zip(baseline_list[0][1], student_results):
                cols_header.append(str(res[0]))
                true_false.append(str(res[1]) == str(ans[1]))

        return rows_header, cols_header, true_false

    @staticmethod
    def construct_whole_matrix(rows_header, cols_header, student_results, baseline_list):
        data_matrix = list([rows_header])
        for col in range(len(cols_header)):
            data = list()
            data.extend(student_results[col])
            if baseline_list is not None and len(baseline_list) > 0:
                for baseline in baseline_list:
                    data.extend(baseline[1][col][1:])
            data_matrix.append(data)
        data_matrix = np.array(data_matrix).transpose()
        return data_matrix

    @staticmethod
    def handle(student_id, student_results, baseline_list=None, email=False,
               save_pic=True, file_name=None):
        logging.info("正在整理：" + str(student_id))
        if save_pic and file_name is None:
            file_name = str(Job_ID) + "_" + str(student_id) + ".pdf"
            file_name = DirectoryHelper.get_log_name(file_name)

        cur_time = time.strftime(Date_Format)
        title = str(student_id) + "\t" + cur_time

        # 生成行和列的表头
        rows_header, cols_header, true_false = DataHandler.construct_headers(student_results, baseline_list)

        # using html table
        data_matrix = DataHandler.construct_whole_matrix(rows_header, cols_header, student_results, baseline_list)

        html = DataHandler.construct_html_pdf(data_matrix, true_false, title)

        if save_pic:
            try:
                DataHandler.html2pdf(html, file_name)
                logging.info("保存文件成功..." + file_name)
            except Exception as unknown_error:
                logging.error("保存文件失败..." + file_name)
                logging.error(str(unknown_error))
                save_pic = False

        if email:
            try:
                tab = DataHandler.construct_pretty_table(rows_header, cols_header, student_results, baseline_list)
                if Is_QQ:
                    html = DataHandler.construct_html_email(str(tab))
                else:
                    html = str(tab)

                subject = "%s-%s-测试结果-%s" % (Job_ID, student_id, cur_time)
                EmailSender(Server_Addr, Admin, Password).send(
                    to=student_id,
                    subject=subject,
                    text=html,
                    attachment=file_name if save_pic else None,
                    is_qq=Is_QQ,
                    html=Is_QQ
                ).close()
                logging.info("发送邮件成功...")
                logging.info("接收邮箱：" + EmailSender.get_email_addr(student_id, is_qq=Is_QQ))
                logging.info("主题：" + subject)
                logging.info("正文：\n" + html)
                if save_pic:
                    logging.info("附件：" + file_name)
            except Exception as unknown_error:
                logging.error("发送邮件失败...")
                logging.error(str(unknown_error))


class RunningHelper(object):
    @staticmethod
    def do_run_case(exe: str, param: str = None, timeout: int = None):
        if isinstance(exe, tuple):
            timeout = exe[2]
            if timeout == 0:
                timeout = None
            param = exe[1]
            exe = exe[0]
        log_txt = "运行命令：" + "./" + exe + " " + param + " --> "
        try:
            process = subprocess.Popen(["./" + exe, param], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                t = Timer()
                t.start()
                stdout, _ = process.communicate(timeout=timeout)
                used_time = t.tick()
                stdout = str(stdout, encoding="utf-8").strip()
                log_txt += stdout + " " + str(used_time) + "ms"
                logging.info(log_txt)
            except subprocess.TimeoutExpired as timeout_error:
                stdout = Timeout_TEXT
                used_time = Timeout_TEXT
                log_txt += str(Timeout_TEXT)

                if "Windows" in platform.platform():
                    kill_cmd = "taskkill.exe /pid " + str(process.pid) + " /F"
                    subprocess.Popen(kill_cmd,
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
                    log_txt += " 结束当前进程：" + kill_cmd
                else:
                    os.kill(process.pid, signal.SIGKILL)
                    os.waitpid(-1, os.WNOHANG)
                    log_txt += " 结束当前进程：" + str(process.pid)

                logging.warning(log_txt)
        except Exception as unknown_error:
            stdout = str(unknown_error)
            used_time = Unknown_TEXT

            log_txt += str(unknown_error)
            logging.error(log_txt)

        return param, stdout, used_time

    @staticmethod
    def job_batch(list_executables, list_params, processes=1, timeout=None):
        # 无参数的情况
        if list_params is None or len(list_params) == 0:
            processes = 1
            list_params = [""]

        if processes == 0:
            processes = len(list_params)

        logging.info("processes=" + str(processes))

        # iterate every executable file
        for exe in list_executables:
            logging.info("exe=" + str(exe))

            matched = re.match(exe_pat, exe)
            if matched:
                _id = matched.group(1)
                my_scores = list()

                if processes > 1:
                    logging.info("创建进程池...")
                    pool = mp.Pool(processes=processes)
                    # prepare for multi-process
                    func_params = list()
                    for param in list_params:
                        func_params.append((exe, param, timeout))

                    # 提交到进程池
                    results = list()
                    for param in func_params:
                        results.append(pool.apply_async(RunningHelper.do_run_case, param))

                    # 从进程池获取结果
                    for param, result in zip(list_params, results):
                        try:
                            p, stdout, used_time = result.get()
                            my_scores.append([str(p), str(stdout), str(used_time)])
                        except Exception as unknown_error:
                            my_scores.append([str(param), str(unknown_error), Unknown_TEXT])
                            logging.error(str(param) + " " + str(unknown_error))

                    pool.close()
                    pool.join()
                    logging.info("关闭进程池...")
                else:
                    # 单进程执行
                    for param in list_params:
                        p, stdout, used_time = RunningHelper.do_run_case(exe, param, timeout)
                        assert p == param
                        my_scores.append([str(p), str(stdout), str(used_time)])
                yield _id, my_scores
            else:
                logging.error("文件格式错误, 与" + str(exe_pat) + "不匹配")
                yield exe, None


def main():
    # 当前目录为job_id
    global Job_ID
    if Job_ID is None:
        Job_ID = os.path.basename(os.path.abspath(os.path.curdir))

    # 生成当前批次的运行日志文件名
    str_time = time.strftime(Date_Format0)
    print("开始运行...", str_time)
    DirectoryHelper.set_log_dir(Job_ID, str_time)
    if not os.path.exists(DirectoryHelper.log_dir):
        os.makedirs(DirectoryHelper.log_dir)
    assert os.path.exists(DirectoryHelper.log_dir)

    out_file_name = Job_ID + "_" + str_time + ".txt"
    logging.basicConfig(level=logging.INFO,
                        filename=DirectoryHelper.get_log_name(out_file_name),
                        filemode="w+",
                        format='[%(asctime)s] [%(levelname)s] %(message)s',
                        datefmt=Date_Format)

    logging.info(DirectoryHelper.get_log_name(out_file_name) + "\n")

    # 获取所有可执行文件，包括baseline和学生的
    all_executable_files = os.listdir(os.path.curdir)
    all_executable_files = filter(lambda fn: str(fn).startswith(Job_ID) and str(fn).endswith("exe"),
                                  all_executable_files)
    all_executable_files = list(all_executable_files)

    # 读取输入样例
    inputs = list()
    if Input_File is not None:
        try:
            logging.info("输入样例文件：" + Input_File)
            with open(Input_File, encoding="utf-8") as fin:
                for line in fin:
                    line = line.strip()
                    if line.startswith("#"):
                        continue
                    line = line.split("#")[0]
                    inputs.append(line.strip())
            logging.info("输入样例列表：" + str(inputs) + "\n")
        except FileNotFoundError as e:
            logging.fatal("输入文件解析失败：" + str(e))
            sys.exit(0)
    else:
        logging.info("无输入样例文件\n\n")

    # 运行baseline
    baselines = filter(lambda fn: "baseline" in fn, all_executable_files)
    baselines = list(baselines)
    baselines_results = list()

    logging.info("Baselines are Running...")
    for b_id, b_result in RunningHelper.job_batch(baselines, inputs, processes=len(inputs), timeout=None):
        if b_result is None:
            continue
        baselines_results.append([b_id, b_result])

        logging.info("Summary: " + str(b_id) + " --> " + str(b_result) + "\n")

        header = str(b_id) + "  " + time.strftime(Date_Format)
        print(header, b_result)

    # 运行学生的程序
    logging.info("Students are Running...")
    students = filter(lambda fn: "baseline" not in fn, all_executable_files)
    students = list(students)
    students_results = list()

    for stu_id, stu_result in RunningHelper.job_batch(students, inputs, processes=len(inputs), timeout=Timeout):
        if stu_result is None:
            continue
        students_results.append([stu_id, stu_result])

        logging.info("Summary: " + str(stu_id) + " --> " + str(stu_result))

        header = str(stu_id) + "  " + time.strftime(Date_Format)
        print(header, stu_result)
        # 为单个学生生成性能分析表
        DataHandler.handle(stu_id, stu_result, baselines_results, save_pic=True, email=True)

    logging.info("正在生成总分析表...")
    table = list()
    colLabels = list()

    def gen_table(results):
        for result in results:
            single_result = list()
            colLabels.append(result[0])
            for case in result[1]:
                single_result.append(case[1] + " (" + str(case[2]) + "ms)")
            table.append(single_result)

    gen_table(baselines_results)
    gen_table(students_results)

    fig = plt.figure(figsize=(len(table[0]) + 2, len(table) / 6 + 1))
    ax = fig.add_subplot(111)
    ax.table(cellText=table, loc="center",
             rowLabels=colLabels,
             colLabels=inputs)
    ax.axis(False)
    plt.xticks([])
    plt.yticks([])
    plt.savefig(DirectoryHelper.get_log_name(Job_ID + "_" + str_time + ".pdf"))
    plt.show()

    logging.shutdown()


def print_time():
    print(time.strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == "__main__":
    if Scheduled:
        scheduler = BlockingScheduler()
        scheduler.add_job(main, 'cron',
                          hour=Hour,
                          minute=Minute,
                          second=Second,
                          end_date=DDL)
        scheduler.start()
    else:
        main()
