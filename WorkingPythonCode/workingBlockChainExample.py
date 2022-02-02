# -*- coding: utf-8 -*-
"""
Created on Mon Jan  3 17:45:04 2022

@author: Brian
"""

# Module 1 - Create a Blockchain
import datetime
import hashlib
import json
from flask import Flask, jsonify

class Blockchain:
    ### initialize
    def __init__(self):
        self.chain = []
        self.createBlock(proof = 1, previous_hash = '0')

    ### create new block
    def createBlock(self, proof, previous_hash):
        block = {
            'index': len(self.chain) + 1,
            'timestamp': str(datetime.datetime.now()),
            'proof': proof,
            'previous_hash': previous_hash
        }
        self.chain.append(block)
        return block

    ### obtain last block in the chain
    def get_previous_block(self):
        return self.chain[-1]

    ### obtain proof of work for a given block
    def proof_of_work(self, previous_proof):
        new_proof = 1
        check_proof = False
        while (check_proof is False):
            # the hashlib.sha256 function requires a 'b' before the string - thats why '.encode()' is used
            hash_operation = hashlib.sha256(str(new_proof**2 - previous_proof**2).encode()).hexdigest()
            # As soon as we find the operation that results in a hash with 4 leading zeroes, the miner wins
            # The more leading zeroes required, the harder it is to mine a block
            # [:4] gets indexes 0, 1, 2, 3
            if hash_operation[:4] == '0000':
                check_proof = True
            else:
                new_proof += 1
        return new_proof

    def hash(self, block):
        encoded_block = json.dumps(block, sort_keys = True).encode()
        return hashlib.sha256(encoded_block).hexdigest()

    ### check that everything is okay in the blockchain
    def is_chain_valid(self, chain):
        previous_block = chain[0]
        block_index = 1
        while block_index < len(chain):
            block = chain[block_index]
            # if the previous hash of the current block is not the same as the previous block, there is a problem
            if block['previous_hash'] != self.hash(previous_block):
                return False
            # if proof starts with 4 leading zeroes (see get_proof_of_work) it is valid
            previous_proof = previous_block['proof']
            proof = block['proof']
            hash_operation = hashlib.sha256(str(proof**2 - previous_proof**2).encode()).hexdigest()
            if hash_operation[:4] != '0000':
                return False
            previous_block = block
            block_index += 1
        return True

# Creating webapp
# webapp will run on http://127.0.0.1:5000/ by default using Flask
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

# Creating blockchain instance
blockchain = Blockchain()

# Mining a new block
@app.route("/mine_block", methods=['GET'])
def mine_block():
    # first need the proof from the last block in the chain
    previous_block = blockchain.get_previous_block()
    previous_proof = previous_block['proof']
    # get the proof of work
    proof = blockchain.proof_of_work(previous_proof)
    previous_hash = blockchain.hash(previous_block)
    # now we create the block
    block =  blockchain.createBlock(proof, previous_hash)
    response = {'message': 'Block has been mined',
                'index': block['index'],
                'timestamp': block['timestamp'],
                'proof': block['proof'],
                'previous_hash': block['previous_hash']}
    return jsonify(response),200

# Getting the full blockchain
@app.route("/get_chain", methods=['GET'])
def get_chain():
    response = {'chain': blockchain.chain,
                'length': len(blockchain.chain)}
    return jsonify(response),200

# Check that the blockchain is valid
@app.route("/is_valid", methods=['GET'])
def validate_chain():
    message = ''
    is_valid = blockchain.is_chain_valid(blockchain.chain)
    if is_valid:
        message = 'blockchain is valid'
    else:
        message = 'blockchain is invalid'

    response = {
        'message': message
    }
    return jsonify(response), 200

# running the app
app.run(host = '0.0.0.0', port = 5000)






