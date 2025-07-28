#! /usr/bin/python
import os
import glob
import subprocess
import argparse
import sys
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse


# Constants
VERSION = "5.0.0"
TEMP_DIR = "/tmp/STARS"
OUTFILE = os.path.join(TEMP_DIR, "STARS_VAL.sql")
PD = os.path.join(TEMP_DIR, "pd")
PKICKOFF = os.path.join(TEMP_DIR, "pkickoff.sh")
VALFILE = os.path.join(TEMP_DIR, "STARS_VAL.out")
DEFAULT_REPORTS_DIR = "/h/data/local/SUP1BT/reports/"
DEFAULT_TAPE_OUT = "/h/data/local/SUP1BT/tape_out/"


def get_arch():
    """Helper function to set ARCH variable"""
    output = subprocess.Popen(["uname","-p"], shell=False, stdout=subprocess.PIPE).communicate()[0]
    if isinstance(output, bytes):
        output = output.decode("utf-8")
    return output.strip()

ARCH = get_arch()

# Initialize correct input builtin based on python version
# NTCSS 3.40 uses python 2.6.6 and NTCSS 3.50 uses python 3.6.8
try:
    # Python 2
    input = raw_input
except NameError:
    # Python 3
    pass


class BaseFileHandler:
    """Base class for shared file handling methods."""

    @staticmethod
    def set_permissions(filepath, mode, user_id, group_id):
        """
        Set file permissions and ownership.
        :param filepath: File to modify
        :param mode: File permissions
        :param user_id, User ID for file ownership
        :param group_id Group ID for file ownership
        """
        os.chmod(filepath, mode)
        os.chown(filepath, user_id, group_id)

    @staticmethod
    def read_file(infile):
        """
        Reads input file and returns contents of readlines
        :param
        """
        with open(infile, "r") as rf:
            lines = rf.readlines()

        return lines

    @staticmethod
    def write_file(outfile, content):
        with open(outfile, "w") as wf:
            wf.writelines(content)


class Validator(BaseFileHandler):
    """
    Handles all validation functions for the stars_creator script
    This includes:
    - Validatating batch_no input
    - Generating and executing validation SQL scripts.
    - Parsing validation output files
    - Checking parameter (bor_dt, tl_no) consistency
    """
    # SQL template for validation queries
    SQL_TEMPLATE = (
                "set nocount on\n"
                "go\n"
                "select 'VarParmDate: ' + rtrim(cast(cast(last_bor_dt as date) as char)) from dbo.variable_parameters\n"
                "union\n"
                "(select '{0}FY: ' + rtrim(cast(max({1}) as char)) from {0})\n"
                "union\n"
                "(select '{0}TL: ' + rtrim(cast(max(trnsmtl_no) as char)) from {0} where {1} = (select max({1}) from {0}))\n"
                "union\n"
                "(select '{0}TLdt: ' + rtrim(cast(cast(max(trnsmtl_dt) as date) as char)) from {0})\n"
                "union\n"
                "(select 'fin_trnsmtlFY: ' + rtrim(cast(max(fy) as char)) from fin_trnsmtl)\n"
                "union\n"
                "(select 'fin_trnsmtlTL: ' + rtrim(cast(max(trnsmtl_no) as char)) from fin_trnsmtl where fy = (select max(fy) from fin_trnsmtl))\n"
                "union\n"
                "(select 'fin_trnsmtlTLdt: ' + rtrim(cast(cast(max(bor_dt) as date) as char)) from fin_trnsmtl)\n"
                "union\n"
                "(select 'material_request_tblFY: ' + rtrim(cast(max(trnsmtl_fy) as char)) from material_request_tbl)\n"
                "union\n"
                "(select 'material_request_tblTL: ' + rtrim(cast(max(trnsmtl_no) as char)) from material_request_tbl where trnsmtl_fy = (select max(trnsmtl_fy) from material_request_tbl))\n"
                "union\n"
                "(select 'material_request_tblTLdt: ' + rtrim(cast(cast(max(trnsmtl_dt) as date) as char)) from material_request_tbl)\n"
                "go"
            )

    def __init__(self, bor_tbl, bor_tbl_fy_col, is_monthly):
        """
        Initialize the Validator with the BOR table.
        :param bor_tbl: The BOR table to use in SQL queries
        """
        self.bor_tbl = bor_tbl
        self.bor_tbl_fy_col = bor_tbl_fy_col
        self.is_monthly = is_monthly

    def ensure_directories(self):
        """
        Ensure that required directories exist.
        """
        if not os.path.exists(TEMP_DIR):
            os.makedirs(TEMP_DIR)
        self.set_permissions(TEMP_DIR, 0o777, 1000, 1000)

    @staticmethod
    def get_sybase_ocs_path():
        """
        Get the path to the Sybase OCS directory.
        """
        ocs_dirs = glob.glob("/opt/sybase/OCS-*")
        if not ocs_dirs:
            raise RuntimeError("No Sybase OCS directory found.")
        return max(ocs_dirs)  # Assume the highest version is the correct one

    @staticmethod
    def get_last_day_of_month(current_date):
        """
        Get the last day of the current month.
        """
        return (datetime(current_date.year, current_date.month, 1) + relativedelta(months=1, days=-1)).date()

    @staticmethod
    def get_last_day_of_previous_month(current_date):
        """
        Get the last day of the previous month.
        """
        return (datetime(current_date.year, current_date.month, 1) + relativedelta(days=-1)).date()

    def check_varparm_bordt(self, is_monthly, var_parm_bordt, fin_tldt):
        """
        Check and print the BOR date status.
        """
        is_good = (
            (is_monthly == "n" and var_parm_bordt == self.get_last_day_of_previous_month(date.today())) or
            (is_monthly == "y" and var_parm_bordt == fin_tldt == self.get_last_day_of_month(date.today()))
        )

        status = "\nBOR date is good!" if is_good else "\nBOR date is no good!"

        output_parts = [
            "{0:<30}".format(status),
            "\n",
            "{0:<40}".format("variable_parameters.bor_dt = {0}".format(var_parm_bordt)),
            "\n"
        ]
        print(" ".join(output_parts))

    def check_mrdt_bortecdt(self, material_request_tldt, bor_tldt):
        """
        Check and print the material request table and BOR date status.
        """
        status = "material_request_tbl tl date matches {0} tl date.".format(self.bor_tbl) if material_request_tldt == bor_tldt else \
                "material_request_tbl tl date doesn't match {0} tl date.".format(self.bor_tbl)
        output_parts = [
            "{0:<60}".format(status),
            "\n",
            "{0:<60}".format("material_request_tbl tl date = {0}".format(material_request_tldt)),
            "{0:<60}".format("{0} tl date = {1}".format(self.bor_tbl, bor_tldt)),
            "\n"
        ]
        print(" ".join(output_parts))

    def check_tl_no(self, bor_tl_no, fin_tl_no, material_request_tl_no):
        """
        Check and print the TL number status.
        """
        status = "TL numbers match." if bor_tl_no == fin_tl_no == material_request_tl_no else "TL numbers do not match!"
        output_parts = [
            "{0:<30}".format(status),
            "\n",
            "{0:<30}".format("{0} = {1}".format(self.bor_tbl, bor_tl_no)),
            "{0:<30}".format("fin_trnsmtl tl_no = {0}".format(fin_tl_no)),
            "{0:<30}".format("material_request_tbl tl_no = {0}".format(material_request_tl_no)),
            "\n"
        ]
        print(" ".join(output_parts))

    def create_pd_file(self):
        """
        Create the pd file by running the appropriate shell command.
        """
        if ARCH == "x86_64":
            # 3.40
            cmd = ["/h/NTCSSS/bin/getlinkdata", "-q", "ssabtusr"]
            with open(PD, "w") as wf:
                subprocess.call(cmd, stdout=wf, stderr=subprocess.PIPE, shell=False)
        else:
            # 3.30
            get_proc = subprocess.Popen(
                ["/h/NTCSSS/bin/getlinkdata", "rsup", "get"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            grep_proc = subprocess.Popen(
                ["grep", "Second value"],
                stdin=get_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            sed_proc = subprocess.Popen(
                ["sed", "-e", "s/^.*= //"],
                stdin=grep_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            with open(PD, "w") as wf:
                output = sed_proc.communicate()[0]
                if isinstance(output, bytes):
                    output = output.decode("utf-8")

                wf.write(output)

                get_proc.stdout.close()
                grep_proc.stdout.close()
                sed_proc.stdout.close()

        self.set_permissions(PD, 0o755, 1000, 1000)

    def generate_sql_script(self):
        """
        Generate the SQL script for validation.
        """
        sql_text = self.SQL_TEMPLATE.format(self.bor_tbl, self.bor_tbl_fy_col)

        self.write_file(OUTFILE, sql_text)
        self.set_permissions(OUTFILE, 0o755, 1000, 1000)

    def generate_pkickoff_script(self, sybase_ocs_path):
        """
        Generate the pkickoff script for executing the SQL script.
        """
        pkickoff_text = "#!/bin/bash\n\nisql -Ussabtusr -P`cat {0}` -Dsupply -b -i {1} -o {2}".format(PD, OUTFILE, VALFILE)

        self.write_file(PKICKOFF, pkickoff_text)
        self.set_permissions(PKICKOFF, 0o777, 1000, 1000)

        return pkickoff_text

    def execute_pkickoff_script(self):
        """
        Execute the pkickoff script and remove temporary files.
        """
        text = self.generate_pkickoff_script(self.sybase_ocs_path)

        command = ["su", "-", "sybase", "-c", PKICKOFF]
        subprocess.call(command, shell=False)

    def parse_valfile(self):
        """
        Parse the valfile and extract relevant parameters.
        """
        val_lines = self.read_file(VALFILE)

        # Extract relevant parameter values from the file
        var_parm_bordt = parse(val_lines[0].strip().partition(":")[-1])
        bor_tldt = parse(val_lines[3].strip().partition(":")[-1])
        fin_tldt = parse(val_lines[6].strip().partition(":")[-1])
        material_request_tldt = parse(val_lines[9].strip().partition(":")[-1])
        bor_tl_no = val_lines[2].strip().partition(":")[-1]
        fin_tl_no = val_lines[5].strip().partition(":")[-1]
        material_request_tl_no = val_lines[8].strip().partition(":")[-1]
        bor_fy =  val_lines[1].strip().partition(":")[-1]
        fin_fy =  val_lines[4].strip().partition(":")[-1]
        material_request_fy =  val_lines[7].strip().partition(":")[-1]

        return (var_parm_bordt, bor_tldt, fin_tldt, material_request_tldt, bor_tl_no,
                fin_tl_no, material_request_tl_no, bor_fy, fin_fy, material_request_fy)

    def val_parameters(self):
        """
        Main function to validate parameters.
        """
        self.ensure_directories()
        self.create_pd_file()
        self.generate_sql_script()
        self.sybase_ocs_path = self.get_sybase_ocs_path()
        self.generate_pkickoff_script(self.sybase_ocs_path)
        self.execute_pkickoff_script()

        var_parm_bordt, bor_tldt, fin_tldt, material_request_tldt, bor_tl_no, fin_tl_no, material_request_tl_no, bor_fy, fin_fy, mtl_fy = self.parse_valfile()

        if not self.is_monthly:
            self.is_monthly = input("\nIs this a monthly closeout? Enter [y/n]:  ").strip().lower()

        self.check_varparm_bordt(self.is_monthly, var_parm_bordt, fin_tldt)
        self.check_mrdt_bortecdt(material_request_tldt, bor_tldt)
        self.check_tl_no(bor_tl_no, fin_tl_no, material_request_tl_no)

        val_confirmation = None
        while val_confirmation not in ["y", "n"]:
            val_confirmation = input("\nDo you wish to recreate the STARS file? Enter [y/n]:  ").strip().lower()

        val_confirmation = val_confirmation == "y"

        return val_confirmation


class LineParser:
    """Parses lines from JSF404 based on format.
    Formats:
    - X0A/Z0A
    - B1N
    """
    # Dictionary identifying string parsing based on format"
    LINE_FORMATS = {
        "X0A": {
            "di": (6, 9),
            "ri": (12, 15),
            "ms": (18, 19),
            "fsc": (24, 28),
            "niin": ((30, 32), (33, 36), (37, 41)),
            "ui": (44, 46),
            "qty": (50, 55),
            "doc_no": (57, 71),
            "tec": (73, 77),
            "dmd": (80, 81),
            "sig": (86, 87),
            "fc": (91, 93),
            "cog": (96, 98),
            "prj": (101, 104),
            "pri": (107, 109),
            "adv": (113, 115),
            "tn": (118, 121),
            "fy": (127, 128),
            "mv": ((134, 142), (143, 145)),
            "rmks": (148, 163),
        },
        "B1N": {
            "di": (6, 9),
            "doc_no": (12, 26),
            "acrn": (32, 34),
            "jon": (38, 49),
            "exp": (54, 55),
            "occ": (62, 65),
            "mv": (72, 83),
            "gl": (89, 92),
            "cr_db": (98, 99),
            "fy": (103, 104),
            "qty": (110, 115),
            "ui": (117, 119),
            "tn": (121, 124),
            "cog": (126, 128),
            "fsc": (130, 134),
            "niin": (135, 144),
        },
    }

    @staticmethod
    def parse_line(line, format_key):
        """Parse line based on specified format"""

        if format_key not in LineParser.LINE_FORMATS:
            raise ValueError("Unknown format: {0} for LineParser".format(format_key))

        slices = LineParser.LINE_FORMATS[format_key]
        parsed_data = {}

        for k, v in slices.items():
            # Combine segments for fields like niin  or mv
            if isinstance(v, tuple) and isinstance(v[0], tuple):
                parsed_data[k] = "".join(line[start:end] for start, end in v)
            # Single value fields
            else:
                start, end = v
                parsed_data[k] = line[start:end]
        # Process money values mv, smv, and fmv based on format
        mv_raw = parsed_data["mv"]

        if format_key == "X0A":
            parsed_data["fmv"] = abs(int(mv_raw.strip())) if mv_raw.strip().lstrip("-").isdigit() else 0
            parsed_data["smv"] = str(parsed_data["fmv"]).zfill(10)
        elif format_key == "B1N":
            parsed_data["fmv"] = abs(int(mv_raw.lstrip("0"))) if mv_raw.strip("-").lstrip("0").isdigit() else 0

        return parsed_data


class StarsFileCreator(BaseFileHandler):
    """
    Recreate stars output file.

    The following class recreates the stars output file
    from a JSF404 Financial (Live) report and places it
    into the tape_out directory so it can be exported
    from the application as normal.
    """

    def __init__(self, batch_no=None, filepath=None, is_monthly=None, rsup_cfg_lvl=None, validate=False):
        self.batch = None
        self.filepath = None
        self.is_monthly = None
        self.rsup_cfg_lvl = None
        self.validate = None

        # process command line arguments
        self.process_arguments()

        # Determine BOR table based on (Force, Unit) install level
        self._set_bor_tbl()

    def process_arguments(self):
        """Parse command line aruments and flags and set configuration values"""
        parser = argparse.ArgumentParser(description="Recreate stars output file from a JSF404 Financial (Live) report.")

        # batch / path args
        batch_group = parser.add_mutually_exclusive_group()
        batch_group.add_argument("-b", "--batch", type=str, help="Batch number (must be 13 characters starting with JSF404)")
        batch_group.add_argument("-p", "--path", type=str, help="Override default reports and tape_out directory with path to JSF")

        # monthly / daily flags
        type_group = parser.add_mutually_exclusive_group()
        type_group.add_argument("-m", "--monthly", action="store_true", help="Flag to indicate monthly closeout")
        type_group.add_argument("-d", "--daily", action="store_true", help="Flag to indicate daily closeout")

        # rsup config level override
        rsup_cfg_group = parser.add_mutually_exclusive_group()
        rsup_cfg_group.add_argument("-f", "--force", action="store_true", help="Flag to manually override rsup_cfg_lvl type")
        rsup_cfg_group.add_argument("-u", "--unit", action="store_true", help="Flag to manually override rsup_cfg_lvl type")

        # Add flag to run sql validation queries
        parser.add_argument("--val", action="store_true", help="Run the SQL validation scripts for TL# and val_parameters date")

        # Add version information
        parser.add_argument("-V", "--version", action="version", version="%(prog)s {0}".format(VERSION), help="Display the version.")

        # process arguments
        args = parser.parse_args()

        self.batch_no = args.batch
        self.is_monthly = "y" if args.monthly else "n" if args.daily else None
        self.rsup_cfg_lvl = "Force" if args.force else "Unit" if args.unit else self.get_rsup_cfg_lvl()
        self.validate = args.val

        if not args.path:
            self.reports_dir = DEFAULT_REPORTS_DIR
            self.tape_out = DEFAULT_TAPE_OUT
            self.latest_report = max(glob.glob(os.path.join(self.reports_dir, "JSF404*")), key=os.path.getmtime)
        else:
            self.latest_report = os.path.abspath(args.path)

            self.reports_dir = os.path.dirname(self.latest_report)
            self.tape_out = os.getcwd()

        self.report_date = os.path.getmtime(self.latest_report)
        self.latest_batch_no = self.latest_report[-13:]

    def _set_bor_tbl(self):
        """
        Set the BOR table based on the RSupply configuration level.
        """

        if self.rsup_cfg_lvl == "Force":
            bor_tbl = "bor_mo_rpt_tec"
            bor_tbl_fy_col = "fscl_yr"
        elif self.rsup_cfg_lvl == "Unit":
            bor_tbl = "bor_mo_rpt"
            bor_tbl_fy_col = "fiscal_year"
        else:
            self.cleanup()
            self.safe_exit("Cannot identify RSupply level (Force, Unit): unable to run validation SQLs.\n")

        self.bor_tbl = bor_tbl
        self.bor_tbl_fy_col = bor_tbl_fy_col

    def safe_exit(self, message, exit_cd):
        """
        Prints exit message and cleansups directories
        """
        print(message)
        self.cleanup()
        sys.exit(exit_cd)

    def ensure_directories(self):
        """
        Ensure that required directories exist.
        """
        if not os.path.exists(TEMP_DIR):
            os.makedirs(TEMP_DIR)
            self.set_permissions(TEMP_DIR, 0o775, 1000, 1000)
        else:
            self.set_permissions(TEMP_DIR, 0o775, 1000, 1000)

    def get_rsup_cfg_lvl(self):
        """
        Identifies the rsupply configuration level (Force or Unit)
        """
        cmd = ["grep", "CF=", "/var/log/archmod/ntcss.env"]
        process = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, _ = process.communicate()

        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8")

        stdout = stdout.strip()

        cfg_lvl = stdout.split("=")[1]
        return cfg_lvl

    @staticmethod
    def cleanup():
        """
        Removes script related directories
        """
        os.system("rm -rf {0}".format(TEMP_DIR))

    @staticmethod
    def validate_batch_input(batch_no, directory):
        """
        Validates the batch number input by the user.
        The input must be 13 characters and start with JSF404.
        The file with new JSF must exist in the reports directory.
        Allows only 3 attempts before exiting.
        """
        if len(batch_no) != 13 or not batch_no.startswith("JSF404"):
            print("\nSorry, the batch number must be 13 characters long and begin with JSF404")
            return False
        elif os.path.exists(os.path.join(directory, batch_no)):
            print("\nThe batch number entered: {0} will be utilized.".format(batch_no))
            return True
        else:
            print("\nCould not identify batch number, try again.")
            return False

    def create_stars(self, infile, outfile, pid):
        """
        Creates stars output.
        Output from validated batch number in the reports directory.
        """

        header = "HEADERRSUPPLY   {0}\n".format(datetime.now().strftime("%Y%m%d%H%M%S"))
        output_lines = [header]

        original_lines = self.read_file(infile) # Read all lines from the JSF404 Report

        # Initiate variable to hold running total for trailer
        total = 0

        for line in original_lines:
            # Check for X0A/Z0A format
            if ("X0A" in line or "Z0A" in line) and len(line) == 182:
                # Parse lines with X0A or Z0A format
                parsed_data = LineParser.parse_line(line, "X0A")

                # Format parsed_data into single line string for file writing
                formatted_line = (
                    "{di}{ri}{ms}{rmks}{ui}{qty}{doc_no}   {tec}{sig}{fc} {cog}{prj}{pri}   "
                    "{adv}{tn}{fy}{smv}\n" if parsed_data["fsc"] + parsed_data["niin"] == "             " else
                    "{di}{ri}{ms}{fsc}{niin}  {ui}{qty}{doc_no}   {tec}{sig}{fc} {cog}{prj}{pri}   "
                    "{adv}{tn}{fy}{smv}\n"
                ).format(
                    di=parsed_data["di"],
                    ri=parsed_data["ri"],
                    ms=parsed_data["ms"],
                    fsc=parsed_data.get("fsc", ""),
                    niin=parsed_data.get("niin", ""),
                    rmks=parsed_data.get("rmks", ""),
                    ui=parsed_data["ui"],
                    qty=parsed_data["qty"],
                    doc_no=parsed_data["doc_no"],
                    tec=parsed_data["tec"],
                    sig=parsed_data["sig"],
                    fc=parsed_data["fc"],
                    cog=parsed_data["cog"],
                    prj=parsed_data["prj"],
                    pri=parsed_data["pri"],
                    adv=parsed_data["adv"],
                    tn=parsed_data["tn"],
                    fy=parsed_data["fy"],
                    smv=parsed_data["smv"]
                )
            # Check for B1N format
            elif "B1N" in line:
                # Parse lines with B1N format
                parsed_data = LineParser.parse_line(line, "B1N")

                # Format parsed_data into single line string for file writing
                formatted_line = (
                    "{di} RSUPP   {di2}{doc_no} "
                    "{acrn}{jon}{exp}{mv}{gl}{cr_db} "
                    "{fy}{spaces36}{qty}{spaces68}{cog}Y{spaces10}"
                    "{niin}{spaces79}{tn}{fsc}{ui}{occ}\n"
                ).format(
                    di=parsed_data["di"][:2],
                    di2=parsed_data["di"][2],
                    doc_no=parsed_data["doc_no"],
                    acrn=parsed_data["acrn"],
                    jon=parsed_data["jon"],
                    exp=parsed_data["exp"],
                    mv=parsed_data["mv"],
                    gl=parsed_data["gl"],
                    cr_db=parsed_data["cr_db"],
                    fy=parsed_data["fy"],
                    spaces36=" " * 36,
                    qty=parsed_data["qty"],
                    spaces68=" " * 68,
                    cog=parsed_data["cog"],
                    spaces10=" " * 10,
                    niin=parsed_data["niin"],
                    spaces79=" " * 79,
                    tn=parsed_data["tn"],
                    fsc=parsed_data["fsc"],
                    ui=parsed_data["ui"],
                    occ=parsed_data["occ"]
                )
            # Skip lines that do not match known format
            else:
                continue
            # Update running total and append formatted line
            total += parsed_data.get("fmv", 0)
            output_lines.append(formatted_line)

        # Add trailer with record count and total
        rec_count = str(len(output_lines) - 1).zfill(15)  # Excluding header row
        trailer = "TRAILERSTARSFL   {0}{1}stars{2}r.txt\n".format(rec_count, str(total).zfill(19), pid)
        output_lines.append(trailer)

        # Write stars output file
        self.write_file(outfile, output_lines)

    def run_stars_creator(self):
        """
        Runs the stars creator process.
        """
        # Identify lasted batch job if no argument for batch_no passed
        if not self.batch_no:
            if os.path.exists(self.latest_report):
                print("\n\nThe latest Financial LIVE batch id is:\n\n{0} run on {1}\n".format(
                    self.latest_batch_no, datetime.fromtimestamp(self.report_date).strftime("%b-%d-%Y @ %H:%M:%S")
                    )
                )

                # Get user confirmation for batch_no
                confirmation = input("\nIs this the correct batch id you are looking for?\n\nEnter Y if yes, or N if no, or Q to quit: ").strip().lower()

                if confirmation in ("y", "yes"):
                    self.batch_no = self.latest_batch_no.upper()
                # Allow user to input a different batch_no than latest
                elif confirmation in ("n", "no"):
                    attempts = 0
                    while attempts < 3:
                        self.batch_no = input("Please enter the correct Job Batch No: ").strip().upper()
                        if self.validate_batch_input(self.batch_no, self.reports_dir):
                            break
                        attempts += 1
                    else:
                        self.safe_exit("Maximum number of attempts reached. Exiting program.", 1)
                elif confirmation in ("q", "quit"):
                    self.safe_exit("Exiting program...", 0)
                else:
                    print("\nCould not identify batch number, try again.")
                    return
            else:
                print("No valid JSF404 file found")
                return

        # Prompt user for validation queries if --val argument not passed
        if not self.validate:
            user_resp = None
            while user_resp not in ["y", "n"]:
                user_resp = input("\nWould you like to run bor_dt and tl_no validation queries? [y/n]: ").strip().lower()

            if user_resp == "y":
                self.validate = True

        # Run SQL validation queries for bor_dt and tl_no if --val argument passed
        if self.validate:
            validator = Validator(self.bor_tbl, self.bor_tbl_fy_col, self.is_monthly)
            val_confirmation = validator.val_parameters()

            if not val_confirmation:
                self.safe_exit("Exiting program...", 1)

        # Proceed to stars file creation
        infile = os.path.join(self.reports_dir, self.batch_no)
        pid = self.batch_no[-3:]
        outfile = os.path.join(self.tape_out, self.batch_no + "RR")
        self.create_stars(infile, outfile, pid)
        self.set_permissions(outfile, 0o660, os.geteuid(), 1001)
        self.cleanup()
        print("\nStars output file is complete and located in the tape_out directory.\nDBA can export file from RSupply via normal methods.\n")


def main():
    """
    Main function to run the StarsFileCreator.
    """
    # Initialize StarsFileCreator
    creator = StarsFileCreator()

    # Run the stars creation process
    creator.run_stars_creator()

if __name__ == "__main__":
    main()
