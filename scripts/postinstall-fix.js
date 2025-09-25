#!/usr/bin/env node
// Ensure kafkajs-msk-iam-authentication-mechanism exposes lib/ after install (workaround nested tgz packaging)
import { existsSync } from 'fs';
import { execSync } from 'child_process';
import path from 'path';

const pkgRoot = path.resolve('node_modules/kafkajs-msk-iam-authentication-mechanism');
const libDir = path.join(pkgRoot, 'lib');
const nestedTgz = path.join(pkgRoot, 'jm18457-kafkajs-msk-iam-authentication-mechanism-3.0.0.tgz');

try {
  if (!existsSync(libDir) && existsSync(nestedTgz)) {
    console.log('[postinstall] Extracting MSK IAM plugin lib/ ...');
    execSync(`tar -xzf ${nestedTgz} -C ${pkgRoot}`);
    execSync(`cp -R ${pkgRoot}/package/lib ${libDir}`);
    console.log('[postinstall] Extraction completed.');
  }
} catch (e) {
  console.warn('[postinstall] Failed to prepare plugin:', e.message);
}
