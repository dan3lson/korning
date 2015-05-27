# Use this file to import the sales information into the
# the database.

require "pg"
require "pry"
require "CSV"

def sales_csv
  invoices = []
  CSV.foreach("sales.csv", headers: true) do |row|
    sale = {}

    email = row[0].split(" ")
    email = remove_parentheses(email[2])
    sale[:employee] = email # to just email
    #sale[:employee] = row[0]

    # emails = db_connection { |conn| conn.exec(
    #   "SELECT email FROM employees"
    # ) }
    a = {
      1 => "clancy.wiggum@korning.com",
      2 => "ricky.bobby@korning.com",
      3 => "bob.lob@korning.com",
      4 => "willie.groundskeeper@korning.com"
    }


    a.each do |id, name|
      sale[:employee] = a.key(name) if email == name
    end


    customer = row[1]
    customer = customer.split(" ")[0]
    sale[:customer] = customer
    #sale[:customer] = row[1] # to just company or company id

    b = {
      1 => "Motorola",
      2 => "LG",
      3 => "HTC",
      4 => "Nokia",
      5 => "Samsung",
      6 => "Apple"
    }

    b.each do |id, name|
      sale[:customer] = b.key(name) if customer == name
    end

    #binding.pry
    #sale[:product] = row[2]

    d = {
      1 => "Baboon Glass",
      2 => "Chimp Glass",
      3 => "Gorilla Glass",
      4 => "Orangutan"
    }

    d.each do |id, name|
      sale[:product] = d.key(name) if row[2] == name
    end

    sale[:day] = row[3]
    sale[:amount] = row[4]
    sale[:units] = row[5]
    sale[:invoice_number] = row[6]

    c = {
      1 => "Monthly",
      2 => "Once",
      3 => "Quarterly",
    }

    c.each do |id, name|
      sale[:frequency] = c.key(name) if row[7] == name
    end
    # sale[:frequency] = row[7]
    invoices << sale
  end
  invoices
end

def select_all(column)
  column_data = []
  sales_csv.each do |invoice|
    column_data << invoice[column]
  end
  column_data
end

def remove_parentheses(string)
  string[0] = ""
  string[-1] = ""
  string
end

def remove_duplication(symbol)
  uniq_records = []
  select_all(symbol).each do |record|
    uniq_records << record unless uniq_records.include?(record)
  end
  uniq_records
end

def import_employees
  master_list = []
  id_number = 1

  remove_duplication(:employee).each do |record|
    uniq_employee = {}
    details = {}
    info = record.split(" ")
    email = info[2]
    remove_parentheses(email)

    details[:first_name] = info[0]
    details[:last_name] = info[1]
    details[:email] = info[2]

    uniq_employee[id_number] = details
    master_list << uniq_employee

    id_number += 1
  end
  master_list
end

def import_customers
  master_list = []
  id_number = 1

  remove_duplication(:customer).each do |record|
    uniq_customer = {}
    details = {}
    info = record.split(" ")

    account_number = info[1]
    remove_parentheses(account_number)

    details[:company] = info[0]
    details[:account_number] = info[1]

    uniq_customer[id_number] = details

    master_list << uniq_customer

    id_number += 1
  end
  master_list
end

def import_products
  master_list = []
  id_number = 1

  remove_duplication(:product).each do |record|
    uniq_product = {}

    details = {}
    details[:name] = record

    uniq_product[id_number] = details

    master_list << uniq_product

    id_number += 1
  end
  master_list
end

def import_day
  master_list = []
  id_number = 1

  select_all(:day).each do |record|
    uniq_day = {}
    details = {}

    details[:day] = record

    uniq_day[id_number] = details

    master_list << uniq_day

    id_number += 1
  end
  master_list
end

def import_amounts
  master_list = []
  id_number = 1
  select_all(:amount).each do |record|
    uniq_amount = {}
    details = {}

    record[0] = ""
    details[:amount] = record.to_f

    uniq_amount[id_number] = details

    master_list << uniq_amount

    id_number += 1
  end
  master_list
end

def import_units
  master_list = []
  id_number = 1
  select_all(:units).each do |record|
    uniq_no_of_units = {}
    details = {}

    details[:units] = record

    uniq_no_of_units[id_number] = details

    master_list << uniq_no_of_units

    id_number += 1
  end
  master_list
end

def import_invoice_numbers
  master_list = []
  id_number = 1
  select_all(:invoice_number).each do |record|
    uniq_invoice_no = {}
    details = {}

    details[:invoice_number] = record

    uniq_invoice_no[id_number] = details

    master_list << uniq_invoice_no

    id_number += 1
  end
  master_list
end

def import_invoice_frequencies
  master_list = []
  id_number = 1

  remove_duplication(:frequency).each do |record|
    uniq_frequency = {}
    details = {}

    details[:frequency] = record

    uniq_frequency[id_number] = details

    master_list << uniq_frequency

    id_number += 1
  end
  master_list
end

def db_connection
  begin
    connection = PG.connect(dbname: "korning")
    yield(connection)
  ensure
    connection.close
  end
end

def normalize_employees
  import_employees.each do |employee|
    id = employee.keys.first
    employee_id = employee[id]
    first_name = employee_id[:first_name]
    last_name = employee_id[:last_name]
    email = employee_id[:email]
    sql = "INSERT INTO employees (
      first_name,
      last_name,
      email )
      VALUES ($1, $2, $3)"
    db_connection { |conn| conn.exec(sql, [
      first_name,
      last_name,
      email]
    ) }
  end
end

def normalize_customers
  import_customers.each do |customer|
    id = customer.keys.first
    customer_id = customer[id]
    company = customer_id[:company]
    account_number = customer_id[:account_number]
    sql = "INSERT INTO customers (
      company,
      account_number )
      VALUES ($1, $2)"
    db_connection { |conn| conn.exec(sql, [
      company,
      account_number]
    ) }
  end
end

def normalize_products
  import_products.each do |product|
    id = product.keys.first
    product_id = product[id]
    name = product_id[:name]
    sql = "INSERT INTO products (
      name )
      VALUES ($1)"
    db_connection { |conn| conn.exec(sql, [
      name]
    ) }
  end
end

def normalize_invoice_frequencies
  import_invoice_frequencies.each do |frequency|
    id = frequency.keys.first
    frequency_id = frequency[id]
    name = frequency_id[:frequency]
    sql = "INSERT INTO invoice_frequencies (
      frequency )
      VALUES ($1)"
    db_connection { |conn| conn.exec(sql, [name]) }
  end
end

def normalize_tables
  normalize_employees
  normalize_customers
  normalize_products
  normalize_invoice_frequencies
end

def normalize_db
  sales_csv.each do |invoice|
    employee_id = invoice[:employee]
    customer_id = invoice[:customer]
    product = invoice[:product]
    day = invoice[:day]
    amount = invoice[:amount]
    units = invoice[:units]
    invoice_number = invoice[:invoice_number]
    frequency_id = invoice[:frequency]

    sql = "INSERT INTO sales (
      day,
      amount,
      units,
      invoice_number,
      employee_id,
      customer_id,
      product_id,
      invoice_frequency_id )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
    db_connection { |conn| conn.exec(sql, [
      day,
      amount,
      units,
      employee_id,
      customer_id,
      product,
      invoice_number,
      frequency_id]
    ) }
  end
end

#puts sales_csv
#normalize_tables
normalize_db
