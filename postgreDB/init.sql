CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    categoryname VARCHAR(255),
    description TEXT,
    picture BYTEA
);

CREATE TABLE customers (
    customer_id VARCHAR(255) PRIMARY KEY,
    companyname VARCHAR(255),
    contactname VARCHAR(255),
    contacttitle VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    region VARCHAR(255),
    postalcode VARCHAR(255),
    country VARCHAR(255),
    phone VARCHAR(255),
    fax VARCHAR(255)
);

CREATE TABLE employee_territories (
    employee_id INT,
    territory_id VARCHAR(255),
    PRIMARY KEY (employee_id, territory_id)
);

CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    lastname VARCHAR(255),
    firstname VARCHAR(255),
    title VARCHAR(255),
    titleofcourtesy VARCHAR(255),
    birthdate DATE,
    hiredate DATE,
    address VARCHAR(255),
    city VARCHAR(255),
    region VARCHAR(255),
    postalcode VARCHAR(255),
    country VARCHAR(255),
    homephone VARCHAR(255),
    extension VARCHAR(255),
    photo BYTEA,
    notes TEXT,
    reportsto INT,
    photopath VARCHAR(255)
);

CREATE TABLE order_details (
    order_id INT,
    product_id INT,
    unitprice FLOAT,
    quantity INT,
    discount FLOAT,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(255),
    employee_id INT,
    orderdate DATE,
    requireddate DATE,
    shippeddate DATE,
    shipvia INT,
    freight FLOAT,
    shipname VARCHAR(255),
    shipaddress VARCHAR(255),
    shipcity VARCHAR(255),
    shipregion VARCHAR(255),
    shippostalcode VARCHAR(255),
    shipcountry VARCHAR(255)
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    productname VARCHAR(255),
    supplier_id INT,
    category_id INT,
    quantityperunit VARCHAR(255),
    unitprice FLOAT,
    unitsinstock INT,
    unitsonorder INT,
    reorderlevel INT,
    discontinued INT
);

CREATE TABLE regions (
    region_id INT PRIMARY KEY,
    regiondescription VARCHAR(255)
);

CREATE TABLE shippers (
    shipper_id SERIAL PRIMARY KEY,
    companyname VARCHAR(255),
    phone VARCHAR(255)
);

CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    companyname VARCHAR(255),
    contactname VARCHAR(255),
    contacttitle VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    region VARCHAR(255),
    postalcode VARCHAR(255),
    country VARCHAR(255),
    phone VARCHAR(255),
    fax VARCHAR(255),
    homepage TEXT
);

CREATE TABLE territories (
    territory_id VARCHAR(255) PRIMARY KEY,
    territorydescription VARCHAR(255),
    region_id INT
);