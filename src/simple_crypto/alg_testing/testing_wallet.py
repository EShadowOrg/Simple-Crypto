class TestWallet:
    def __init__(self, initial_balance=0, currency='USD'):
        self.balance = initial_balance
        self.currency = currency
        self.coins = {}

    def deposit(self, amount):
        if amount > 0:
            self.balance += amount
            return True
        return False

    def buy(self, symbol, amount, cost):
        if self.balance >= cost:
            self.balance -= cost
            self.coins[symbol] = self.coins.get(symbol, 0) + amount
            return True
        return False

    def sell(self, symbol, amount, revenue):
        if symbol in self.coins and self.coins[symbol] >= amount:
            self.coins[symbol] -= amount
            if self.coins[symbol] == 0:
                del self.coins[symbol]
            self.balance += revenue
            return True
        return False