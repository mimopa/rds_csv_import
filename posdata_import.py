from dotenv import load_dotenv
import os
import csv
import mysql.connector as mydb

# RDS MySQLへのコネクション作成
rds = os.environ["RDS_JDMC"]

con = mydb.connect(
    host=rds,
    port=os.environ["RDS_JDMC_PORT"],
    user=os.environ["RDS_JDMC_USER"],
    password=os.environ["RDS_JDMC_PASSWORD"],
    database=os.environ["RDS_JDMC_DATABASE"],
    charset='utf8'
)

# 例外処理
try:
    cur = con.cursor()

    with open('IDPOS_20190906172907_307368.csv', encoding="utf_8") as f:
        reader = csv.reader(f)
        heder = next(reader, None)

        cur.execute("DROP TABLE IF EXISTS `idpos_table_csv`")
        cur.execute("""CREATE TABLE IF NOT EXISTS `idpos_table_csv`(
                        `_id` INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        `ymd` INT(8),
                        `hour` INT(2),
                        `storeCd` INT(4),
                        `storeName` VARCHAR(50),
                        `divisionCd` INT(4),
                        `divisionName` VARCHAR(50),
                        `lineCd` INT(4),
                        `lineName` VARCHAR(50),
                        `departmentCd` INT(4),
                        `departmentName` VARCHAR(50),
                        `categoryCd` INT(4),
                        `categoryName` VARCHAR(50),
                        `subCategoryCd` INT(4),
                        `subCategoryName` VARCHAR(50),
                        `segmentCd` INT(4),
                        `segmentName` VARCHAR(50),
                        `subSegmentCd` INT(4),
                        `subSegmentName` VARCHAR(50),
                        `JANCd` VARCHAR(20),
                        `productName` VARCHAR(100),
                        `specification` VARCHAR(50),
                        `maker` VARCHAR(100),
                        `memberId` VARCHAR(20),
                        `sex` VARCHAR(10),
                        `ageRange` VARCHAR(10),
                        `visitDistanceLeKm` VARCHAR(10),
                        `unitPrice` NUMERIC(11,2),
                        `salesVolume` INT(4),
                        `salesTaxIncluded` NUMERIC(11,2),
                        `argumentAmount` NUMERIC(11,2),
                        `dincountAmount` NUMERIC(11,2)) character set utf8mb4;""")
        con.commit()

        sql = "INSERT INTO idpos_table_csv (ymd, hour, storeCd, storeName, divisionCd, divisionName, lineCd, lineName, departmentCd, departmentName, categoryCd, categoryName, subCategoryCd, subCategoryName, segmentCd, segmentName, subSegmentCd, subSegmentName, JANCd, productName, specification, maker, memberId, sex, ageRange, visitDistanceLeKm, unitPrice, salesVolume, salesTaxIncluded, argumentAmount, dincountAmount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

        for i in reader:
            # print(i)
            cur.execute(sql, (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10],
                              i[11], i[12], i[13], i[14], i[15], i[16], i[17], i[18], i[19], i[20],
                              i[21], i[22], i[23], i[24], i[25], i[26], i[27], i[28], i[29], i[30]))
        con.commit()

except mydb.Error as e:
    print("Error code:", e.errno)  # error number
    print("SQLSTATE value:", e.sqlstate)  # SQLSTATE value
    print("Error message:", e.msg)  # error message
    print("Error:", e)  # errno, sqlstate, msg values
    s = str(e)
    print("Error:", s)
