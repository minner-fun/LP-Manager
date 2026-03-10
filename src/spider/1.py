from web3 import Web3, AsyncWeb3
from src.Constracts import MAINNET_RPC_URL
w3 = Web3(Web3.HTTPProvider(MAINNET_RPC_URL))

latest = w3.eth.get_block("latest")
print(latest)