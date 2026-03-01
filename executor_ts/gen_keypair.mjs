import fs from "fs";
import path from "path";
import { Keypair } from "@solana/web3.js";

const kp = Keypair.generate();

// хотим создать id.json в корне проекта, рядом с main.py
const outPath = path.resolve(process.cwd(), "..", "id.json");

fs.writeFileSync(outPath, JSON.stringify(Array.from(kp.secretKey)));
console.log("Created:", outPath);
console.log("Public key:", kp.publicKey.toBase58());
