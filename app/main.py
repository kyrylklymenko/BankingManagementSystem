import decimal

from redis import Redis
import datetime, string, random, requests
from decimal import Decimal

from flask import Flask, render_template, redirect, url_for, request, flash
from flask_login import LoginManager, login_user, logout_user, current_user
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from werkzeug.security import generate_password_hash, check_password_hash
from flask_user import roles_required, SQLAlchemyAdapter, UserManager, UserMixin, login_required

# from flask_user import UserMixin

app = Flask(__name__)
manager = LoginManager(app)
manager.init_app(app)
manager.login_view = 'login'

redis_creds = Redis()
username = redis_creds.get("keystore:postgres:username").decode("utf-8")
password = redis_creds.get("keystore:postgres:password").decode("utf-8")

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + username + ':' + password + '@localhost:5432/dbms_banking'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'Key'

db = SQLAlchemy(app)
db.create_all()

DEPOSIT_MONTH_INTEREST = 0.05


# Models


class Users(db.Model, UserMixin):
    __tablename__ = 'users'

    id = db.Column(db.INTEGER, primary_key=True, name='user_id', autoincrement=True)
    first_name = db.Column(db.String(40), nullable=False)
    last_name = db.Column(db.String(40), nullable=False)
    user_password = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(50), nullable=False)
    date_created = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow())
    deposit_profit = db.Column(db.NUMERIC(20, 2), nullable=False, default=decimal.Decimal("0.00"))
    roles = db.relationship('Roles', secondary='user_roles', backref=db.backref('users', lazy='dynamic'))

    def __repr__(self):
        return "<User {}>".format(self.user_id)


class Roles(db.Model):
    __tablename__ = 'roles'

    id = db.Column(db.SMALLINT, primary_key=True)
    name = db.Column(db.String(50), nullable=False, unique=True, name='role_name')


class UserRoles(db.Model):
    __tablename__ = 'user_roles'

    id = db.Column(db.INTEGER, primary_key=True)
    user_id = db.Column(db.INTEGER, db.ForeignKey('users.user_id', ondelete='CASCADE', onupdate='CASCADE'))
    user_role = db.Column(db.INTEGER, db.ForeignKey('roles.id', ondelete='CASCADE', onupdate='CASCADE'))


class DebitCards(db.Model):
    __tablename__ = 'debit_cards'
    card_id = db.Column(db.INTEGER, primary_key=True, autoincrement=True)
    card_num = db.Column(db.String(16), nullable=False)
    opening_date = db.Column(db.TIMESTAMP, nullable=False, default=datetime.datetime.utcnow())
    balance = db.Column(db.NUMERIC(20, 2), nullable=False)
    exp_month = db.Column(db.SMALLINT, nullable=False)
    exp_year = db.Column(db.SMALLINT, nullable=False)
    cvc = db.Column(db.SMALLINT, nullable=False)
    card_owner_id = db.Column(db.INTEGER, db.ForeignKey('users.user_id', ondelete='CASCADE', onupdate='CASCADE'))
    card_currency = db.Column(db.SMALLINT, db.ForeignKey('currency_cashbook.currency_id'))


class Deposits(db.Model):
    __tablename__ = 'deposits'

    deposit_id = db.Column(db.INTEGER, primary_key=True, autoincrement=True)
    date_created = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow())
    balance = db.Column(db.NUMERIC(20, 2), nullable=False)
    deposit_owner_id = db.Column(db.INTEGER, db.ForeignKey('users.user_id', ondelete='CASCADE', onupdate='CASCADE'))
    currency = db.Column(db.SMALLINT, db.ForeignKey('currency_cashbook.currency_id'))


class CurrencyCashbook(db.Model):
    __tablename__ = 'currency_cashbook'

    currency_id = db.Column(db.INTEGER, primary_key=True)
    currency_code = db.Column(db.String(3), nullable=False)
    usd_cross_rate = db.Column(db.Numeric(10, 2), nullable=False)


class BankMoneyAccount(db.Model):
    __tablename__ = 'bank_money_account'

    id = db.Column(db.INTEGER, primary_key=True)
    currency_id = db.Column(db.INTEGER, db.ForeignKey('currency_cashbook.currency_id'), nullable=False)
    balance = db.Column(db.NUMERIC(20, 2), nullable=False)


class OperationStatus(db.Model):
    __tablename__ = 'operation_status'

    status_id = db.Column(db.INTEGER, primary_key=True)
    status_name = db.Column(db.String(50), nullable=False)


class DepositStatus(db.Model):
    __tablename__ = 'deposit_status'

    status_id = db.Column(db.INTEGER, primary_key=True)
    status_name = db.Column(db.String(50), nullable=False)


class DebitCardStatus(db.Model):
    __tablename__ = 'debit_card_status'

    status_id = db.Column(db.INTEGER, primary_key=True)
    status_name = db.Column(db.String(50), nullable=False)


class DebitCardOperationLogs(db.Model):
    __tablename__ = 'debit_card_operation_logs'

    operation_id = db.Column(db.INTEGER, primary_key=True, autoincrement=True)
    client_id = db.Column(db.INTEGER, nullable=False)
    balance_change = db.Column(db.NUMERIC(20, 2), nullable=False)
    currency = db.Column(db.INTEGER, db.ForeignKey('currency_cashbook.currency_id'), nullable=False)
    card_status = db.Column(db.INTEGER, db.ForeignKey('debit_card_status.status_id'), nullable=False)
    operation_status = db.Column(db.INTEGER, db.ForeignKey('operation_status.status_id'), nullable=False)
    card_id = db.Column(db.INTEGER)
    operation_date = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow())


class DepositOperationLogs(db.Model):
    __tablename__ = 'deposit_operation_logs'

    operation_id = db.Column(db.INTEGER, primary_key=True, autoincrement=True)
    client_id = db.Column(db.INTEGER, nullable=False)
    balance = db.Column(db.NUMERIC(20, 2), nullable=False)
    currency = db.Column(db.INTEGER, db.ForeignKey('currency_cashbook.currency_id'), nullable=False)
    deposit_status = db.Column(db.INTEGER, db.ForeignKey('deposit_status.status_id'), nullable=False)
    operation_status = db.Column(db.INTEGER, db.ForeignKey('operation_status.status_id'), nullable=False)
    operation_date = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow(), nullable=False)


db_adapter = SQLAlchemyAdapter(db, Users)
user_manager = UserManager(db_adapter, app)


@app.route('/')
def index():
    return render_template("index.html")


@app.route('/service_info')
def services_info():
    return render_template("service_info.html")


@app.route('/account')
@login_required
def account():
    role = current_user.roles[0]
    return render_template('account_page.html', role=role)


@app.route('/currency_info')
def currency_info():
    r = requests.get("https://api.privatbank.ua/p24api/pubinfo?json&exchange&coursid=5")
    data = r.json()
    return render_template("currency_info.html", data=data)


@app.route("/login", methods=['GET', 'POST'])
def login():
    email = request.form.get('email')
    password = request.form.get('password')
    user = Users.query.filter_by(email=email).first()
    if request.method == "POST":
        if user and check_password_hash(user.user_password, password):
            login_user(user)
            return redirect(url_for("index"))
        else:
            flash("Введено невірні дані пошти чи паролю. Просимо Вас ввести дані ще раз!")

    return render_template('login.html')


@app.route("/register", methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        first_name = request.form.get('first_name')
        last_name = request.form.get('last_name')
        user_password = request.form.get('password')
        email = request.form.get('email')
        role = request.form.get('role')
        if Users.query.filter_by(email=email).count() == 1:
            flash("Користувач з такою поштою вже існує. Просимо Вас ввести дані ще раз!")
        else:
            pass_hash = generate_password_hash(user_password)
            new_user = Users(first_name=first_name, last_name=last_name,
                             user_password=pass_hash, email=email)
            db.session.add(new_user)
            db.session.commit()

            user_role = UserRoles(user_id=new_user.id, user_role=int(role))
            db.session.add(user_role)
            db.session.commit()

            return redirect(url_for('register_success'))

    if request.method == 'GET':
        print("Just getting the page")
    return render_template('register.html')


@app.route("/register_success")
def register_success():
    return render_template('register_success.html')


@app.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    if request.method == "POST":
        old_password = request.form.get('old_password')
        new_password = request.form.get('password')
        if not check_password_hash(current_user.user_password, old_password):
            flash("Невірний поточний пароль")
        else:
            user = Users.query.filter_by(id=current_user.id).first()
            user.user_password = generate_password_hash(new_password)
            db.session.commit()
            return redirect(url_for("account"))
    return render_template("change_password.html")


@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for("index"))


@app.route('/bank_account')
@login_required
@roles_required('Administrator')
def bank_account():
    BANK_MONEY_ACCOUNTS = f'''
    select c.currency_code, acc.balance from
    bank_money_account acc left join currency_cashbook c
    on acc.currency_id = c.currency_id'''
    bank_accounts = db.engine.execute(BANK_MONEY_ACCOUNTS)
    accounts = []
    for acc in bank_accounts:
        accounts.append({
            'currency': acc.currency_code,
            'balance': acc.balance
        })
    return render_template("bank_money_account.html", accounts=accounts)


@app.route('/manage_users')
@login_required
@roles_required('Administrator')
def manage_users():
    all_users = Users.query.filter(Users.email != current_user.email).all()
    return render_template('manage_users.html', all_users=all_users)


@app.route('/add_user', methods=["GET", "POST"])
@login_required
@roles_required('Administrator')
def add_user():
    if request.method == "POST":
        first_name = request.form.get('first_name')
        last_name = request.form.get('last_name')
        user_password = request.form.get('password')
        email = request.form.get('email')
        role = request.form.get('role')
        if Users.query.filter_by(email=email).count() == 1:
            flash("Користувач з такою поштою вже існує")
        else:
            pass_hash = generate_password_hash(user_password)
            new_user = Users(first_name=first_name, last_name=last_name,
                             user_password=pass_hash, email=email)
            db.session.add(new_user)
            db.session.commit()

            user_role = UserRoles(user_id=new_user.id, user_role=int(role))
            db.session.add(user_role)
            db.session.commit()

            return redirect(url_for('manage_users'))
    return render_template('add_user.html')


@app.route('/user/<id>/change_role', methods=["GET", "POST"])
@login_required
@roles_required('Administrator')
def change_role(id):
    user = Users.query.filter_by(id=id).first()
    role = user.roles[0].name
    if role == "Client":
        role = "Клієнт"
    elif role == "Manager":
        role = "Менеджер"
    elif role == "Administrator":
        role = "Адміністратор"

    if request.method == "POST":
        role = UserRoles.query.filter_by(user_id=id).first()
        role.user_role = int(request.form.get("role"))
        db.session.commit()
        return redirect(url_for("manage_users"))
    return render_template("change_role.html", role=role, id=id)


@app.route("/manage_orders")
@login_required
@roles_required('Manager')
def manage_orders():
    deposit_operations = DepositOperationLogs.query.filter_by(operation_status=1).all()
    card_operations = DebitCardOperationLogs.query.filter_by(operation_status=1).all()
    return render_template("manage_orders.html", deposit_operations=deposit_operations, card_operations=card_operations)


@app.route("/deposit/create", methods=["GET", "POST"])
@login_required
@roles_required('Client')
def open_deposit():
    if request.method == "POST":
        currency = int(request.form.get("currency"))
        create_balance = Decimal(request.form.get("balance"))
        currency_acc_balance = BankMoneyAccount.query.filter_by(currency_id=currency).first().balance
        if currency_acc_balance < create_balance:
            flash("У банку немає достатньо коштів для цієї операції")
        else:
            max_id = DepositOperationLogs.query.with_entities(func.max(DepositOperationLogs.operation_id)).first()
            max_id = 0 if max_id[0] is None else max_id[0]
            deposit_creation = DepositOperationLogs(operation_id=max_id + 1, balance=create_balance, operation_status=1,
                                                    deposit_status=1, client_id=current_user.id, currency=currency)
            db.session.add(deposit_creation)
            db.session.commit()
            return redirect(url_for("manage_user_services"))
    return render_template("create_deposit.html")


@app.route("/manage_services")
@login_required
@roles_required('Client')
def manage_user_services():
    DEPOSITS_SCRIPT = f'''
    SELECT d.balance, c.currency_code FROM users u LEFT JOIN deposits d
    ON u.user_id = d.deposit_owner_id
    left join currency_cashbook c
    ON d.currency = c.currency_id
    WHERE u.user_id = {current_user.id}'''
    user_deposits = db.engine.execute(DEPOSITS_SCRIPT)
    deposits = []
    for deposit in user_deposits:
        deposits.append({
            'balance': deposit.balance,
            'currency': deposit.currency_code
        })
    if deposits[0]["balance"] is None:
        deposits = []

    CARDS_SCRIPT = f'''
    SELECT dc.balance, dc.card_id, dc.exp_month, dc.exp_year, dc.cvc, dc.card_num, c.currency_code FROM users u LEFT JOIN debit_cards dc
    ON u.user_id = dc.card_owner_id
    LEFT JOIN currency_cashbook c
    ON dc.card_currency = c.currency_id
    WHERE u.user_id = {current_user.id}'''
    user_cards = db.engine.execute(CARDS_SCRIPT)
    cards = []
    for card in user_cards:
        cards.append({
            'balance': card.balance,
            'card_id': card.card_id,
            'currency': card.currency_code,
            'exp_month': card.exp_month,
            'exp_year': card.exp_year,
            'cvc': card.cvc,
            'card_num': card.card_num,
            'pending_operation': card_operation_pending(card.card_id)
        })
    if cards[0]["balance"] is None:
        cards = []

    has_active_deposit = user_has_active_deposit()
    has_pending_deposit = user_has_pending_deposit()
    print(has_active_deposit and len(deposits) > 0)
    return render_template("manage_services.html", deposits=deposits, cards=cards,
                           has_active_deposit=has_active_deposit, has_pending_deposit=has_pending_deposit)


@app.route("/deposit/delete")
@login_required
@roles_required('Client')
def close_deposit():
    WITHDRAW_BALANCE_SCRIPT = f'''
    select d.balance, d.currency from
    users u left join deposits d
    on u.user_id = d.deposit_owner_id
    where u.user_id = {current_user.id}'''
    withdraw_balances = db.engine.execute(WITHDRAW_BALANCE_SCRIPT)
    for b in withdraw_balances:
        withdraw_balance = b.balance
        currency = b.currency

    max_id = DepositOperationLogs.query.with_entities(func.max(DepositOperationLogs.operation_id)).first()
    max_id = 0 if max_id[0] is None else max_id[0]
    deposit_delete = DepositOperationLogs(operation_id=max_id + 1, balance=withdraw_balance, operation_status=1,
                                          deposit_status=2, client_id=current_user.id, currency=currency)
    db.session.add(deposit_delete)
    db.session.commit()
    return redirect(url_for("manage_user_services"))


@app.route('/card/create', methods=["GET", "POST"])
@login_required
@roles_required('Client')
def open_card():
    if request.method == "POST":
        currency = int(request.form.get("currency"))
        max_id = DebitCardOperationLogs.query.with_entities(func.max(DebitCardOperationLogs.operation_id)).first()
        max_id = 0 if max_id[0] is None else max_id[0]
        card_creation = DebitCardOperationLogs(operation_id=max_id + 1, balance_change=0, operation_status=1,
                                               card_status=1, client_id=current_user.id, currency=currency)

        db.session.add(card_creation)
        db.session.commit()
        return redirect(url_for("manage_user_services"))
    return render_template("create_card.html")


@app.route('/card/<id>/delete', methods=["GET", "POST"])
@login_required
@roles_required('Client')
def close_card(id):
    max_id = DebitCardOperationLogs.query.with_entities(func.max(DebitCardOperationLogs.operation_id)).first()
    max_id = 0 if max_id[0] is None else max_id[0]
    card_delete = DebitCardOperationLogs(operation_id=max_id + 1, balance_change=0, operation_status=1,
                                         card_id=id, card_status=2, client_id=current_user.id)
    db.session.add(card_delete)
    db.session.commit()
    return redirect((url_for("manage_user_services")))


@app.route('/card/<card_id>/withdraw', methods=["GET", "POST"])
@login_required
@roles_required('Client')
def withdraw_card(card_id):
    if request.method == "POST":
        withdraw_value = decimal.Decimal(request.form.get("balance"))
        if withdraw_value > get_card_balance(card_id):
            flash("Коштів на карті недостатньо для здійснення операції")
        else:
            max_id = DebitCardOperationLogs.query.with_entities(func.max(DebitCardOperationLogs.operation_id)).first()
            max_id = 0 if max_id[0] is None else max_id[0]
            withdraw_card_order = DebitCardOperationLogs(operation_id=max_id + 1,
                                                         balance_change=withdraw_value, operation_status=1,
                                                         card_id=card_id, card_status=4, client_id=current_user.id,
                                                         currency=get_card_currency(card_id))
            db.session.add(withdraw_card_order)
            db.session.commit()
            return redirect(url_for("manage_user_services"))
    return render_template("withdraw_card.html", card_id=card_id)


@app.route('/card/<card_id>/replenish', methods=["GET", "POST"])
@login_required
@roles_required('Client')
def replenish_card(card_id):
    if request.method == "POST":
        replenish_value = decimal.Decimal(request.form.get("balance"))
        max_id = DebitCardOperationLogs.query.with_entities(func.max(DebitCardOperationLogs.operation_id)).first()
        max_id = 0 if max_id[0] is None else max_id[0]
        replenish_card_order = DebitCardOperationLogs(operation_id=max_id + 1,
                                                     balance_change=replenish_value, operation_status=1,
                                                     card_id=card_id, card_status=3, client_id=current_user.id,
                                                     currency=get_card_currency(card_id))
        db.session.add(replenish_card_order)
        db.session.commit()
        return redirect(url_for("manage_user_services"))
    return render_template("replenish_card.html", card_id=card_id)


@app.route("/manage_orders/<operation_type>/<operation_result>/<result>/<operation_id>")
@login_required
@roles_required('Manager')
def order_result(operation_type, operation_result, result, operation_id):
    operation_result = int(operation_result)
    operation_id = int(operation_id)
    if operation_type == 'deposit':
        order = DepositOperationLogs.query.filter_by(operation_id=operation_id).first()
        if result == 'reject':
            order.operation_status = 3
            db.session.commit()
        else:
            order.operation_status = 2
            if operation_result == 1:
                max_id = Deposits.query.with_entities(func.max(Deposits.deposit_id)).first()
                max_id = 0 if max_id[0] is None else max_id[0]
                new_deposit = Deposits(deposit_id=max_id, balance=order.balance, deposit_owner_id=order.client_id,
                                       currency=order.currency)
                db.session.add(new_deposit)
                bank_account = BankMoneyAccount.query.filter_by(currency_id=order.currency).first()
                bank_account.balance += order.balance
                db.session.commit()
            elif operation_result == 2:
                deposit = Deposits.query.filter_by(deposit_owner_id=order.client_id).first()
                withdraw_date = order.operation_date
                if int((withdraw_date - deposit.date_created).days / 30) < 1:
                    months = 1
                else:
                    months = int((withdraw_date - deposit.date_created).days / 30)
                multiplier = decimal.Decimal(str((1 + DEPOSIT_MONTH_INTEREST) ** months))
                withdraw_value = (deposit.balance * multiplier).quantize(decimal.Decimal("1.00"), decimal.ROUND_FLOOR)
                db.session.delete(deposit)
                db.session.commit()
                bank_account = BankMoneyAccount.query.filter_by(currency_id=order.currency).first()
                bank_account.balance -= withdraw_value
                add_client_deposit_profit(order.client_id, withdraw_value)
                db.session.commit()
    else:
        order = DebitCardOperationLogs.query.filter_by(operation_id=operation_id).first()
        if result == 'reject':
            order.operation_status = 3
            db.session.commit()
        else:
            order.operation_status = 2
            if operation_result == 1:
                max_id = DebitCards.query.with_entities(func.max(DebitCards.card_id)).first()
                max_id = 0 if max_id[0] is None else max_id[0]
                new_card = DebitCards(card_id=max_id + 1, card_currency=order.currency, card_num=get_random_card_num(),
                                      exp_year=datetime.datetime.now().year + 2,
                                      exp_month=datetime.datetime.now().month,
                                      card_owner_id=order.client_id, balance=0, cvc=get_random_cvc())
                db.session.add(new_card)
                db.session.commit()
            elif operation_result == 2:
                card = DebitCards.query.filter_by(card_id=order.card_id).first()
                db.session.delete(card)
                db.session.commit()
            elif operation_result == 3:
                card = DebitCards.query.filter_by(card_id=order.card_id).first()
                card.balance += order.balance_change
                bank_account = BankMoneyAccount.query.filter_by(currency_id=order.currency).first()
                bank_account.balance += order.balance_change
                db.session.commit()
            elif operation_result == 4:
                card = DebitCards.query.filter_by(card_id=order.card_id).first()
                card.balance -= order.balance_change
                bank_account = BankMoneyAccount.query.filter_by(currency_id=order.currency).first()
                bank_account.balance -= order.balance_change
                db.session.commit()

    return redirect(url_for("manage_orders"))


def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def get_random_card_num():
    numbers = string.digits
    num = ''.join(random.choice(numbers) for i in range(16))
    return num


def get_random_cvc():
    return random.randint(100, 1000)


def user_has_active_deposit():
    HAS_DEPOSIT = f'''
    select * from users u
    right join deposit_operation_logs dlogs
    on dlogs.client_id = u.user_id
    where dlogs.operation_status = 1 and dlogs.deposit_status=2
    and u.user_id = {current_user.id}'''
    has_deposits = db.engine.execute(HAS_DEPOSIT)
    count = 0
    for item in has_deposits:
        count += 1
    return count == 0


def user_has_pending_deposit():
    HAS_DEPOSIT = f'''
    select * from users u
    right join deposit_operation_logs dlogs
    on dlogs.client_id = u.user_id
    where dlogs.operation_status = 1 and dlogs.deposit_status=1
    and u.user_id = {current_user.id}'''
    has_deposits = db.engine.execute(HAS_DEPOSIT)
    count = 0
    for item in has_deposits:
        count += 1
    return count > 0


def card_operation_pending(card_id):
    CARD_OPERATION_PENDING = f'''
    SELECT * FROM users u
    right join debit_card_operation_logs clogs
    on clogs.client_id = u.user_id
    where clogs.operation_status = 1 and (clogs.card_status > 1)
    and clogs.card_id = {'null' if card_id is None else card_id}'''
    is_pendings = db.engine.execute(CARD_OPERATION_PENDING)
    count = 0
    for item in is_pendings:
        count += 1
    return count > 0


# (/manage_orders/deposit or card/close or open or change bal/accept or reject/what operation
def add_client_deposit_profit(client_id, value):
    ADD_DEPOSIT_PROFIT = f'''
    UPDATE users SET deposit_profit = deposit_profit + {value}
    WHERE user_id = {client_id}'''
    db.engine.execute(ADD_DEPOSIT_PROFIT)


def get_card_balance(card_id):
    CARD_BALANCE = f'''
    select c.balance from debit_cards c
    where c.card_id = {card_id}'''
    res = db.engine.execute(CARD_BALANCE)
    for i in res:
        balance = i.balance
    return balance

def get_card_currency(card_id):
    CARD_CURRENCY = f'''
        select c.card_currency from debit_cards c
        where c.card_id = {card_id}'''
    res = db.engine.execute(CARD_CURRENCY)
    for i in res:
        currency = i.card_currency
    return currency


@manager.user_loader
def load_user(user_id):
    return Users.query.get(user_id)


app.run(debug=True)
