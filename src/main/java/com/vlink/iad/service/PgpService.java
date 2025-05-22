package com.vlink.iad.service;

import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.PGPDigestCalculator;
import org.bouncycastle.openpgp.operator.jcajce.*;
import org.springframework.stereotype.Service;

import java.io.*;
import java.security.*;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.util.Date;
import java.util.Base64;

@Service
public class PgpService {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }
	
    private PGPPrivateKey findPrivateKey(InputStream keyIn, long keyID, char[] pass)
            throws IOException, PGPException, NoSuchProviderException {
        PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(keyIn), new JcaKeyFingerprintCalculator());
        PGPSecretKey key = pgpSec.getSecretKey(keyID);

        if (key == null) {
            return null;
        }

        PBESecretKeyDecryptor decryptor = new JcePBESecretKeyDecryptorBuilder(new JcaPGPDigestCalculatorProviderBuilder().build())
                .setProvider("BC").build(pass);
        return key.extractPrivateKey(decryptor);
    }
	
    private PGPPublicKey findPublicKey(InputStream input) throws IOException, PGPException {
        input = PGPUtil.getDecoderStream(input);
        PGPPublicKeyRingCollection pgpPub = new PGPPublicKeyRingCollection(input, new JcaKeyFingerprintCalculator());

        for (PGPPublicKeyRing keyRing : pgpPub) {
            for (PGPPublicKey key : keyRing) {
                if (key.isEncryptionKey()) {
                    return key;
                }
            }
        }
        throw new IllegalArgumentException("Can't find encryption key in key ring.");
    }	

	/*
    public void generateKeyPair(String identity, String passphrase, OutputStream pubOut, OutputStream privOut)
            throws NoSuchAlgorithmException, NoSuchProviderException, PGPException, IOException, SignatureException {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA", "BC");
        keyGen.initialize(2048);
        KeyPair keyPair = keyGen.generateKeyPair();

        PGPDigestCalculator sha1Calc = new JcaPGPDigestCalculatorProviderBuilder().build().get(PGPUtil.SHA1);

        PGPKeyPair pgpKeyPair = new JcaPGPKeyPair(PGPPublicKey.RSA_GENERAL, keyPair, new Date());

        PGPKeyRingGenerator keyRingGen = new PGPKeyRingGenerator(
                PGPSignature.POSITIVE_CERTIFICATION,
                pgpKeyPair,
                identity,
                sha1Calc,
                null,
                null,
                new JcaPGPContentSignerBuilder(pgpKeyPair.getPublicKey().getAlgorithm(), PGPUtil.SHA1),
                new JcePBESecretKeyEncryptorBuilder(PGPEncryptedData.CAST5, sha1Calc).setProvider("BC").build(passphrase.toCharArray())
        );

        PGPPublicKeyRing pubKeyRing = keyRingGen.generatePublicKeyRing();
        PGPSecretKeyRing secKeyRing = keyRingGen.generateSecretKeyRing();

        try (ArmoredOutputStream armoredOut = new ArmoredOutputStream(pubOut)) {
            pubKeyRing.encode(armoredOut);
        }

        try (ArmoredOutputStream armoredOut = new ArmoredOutputStream(privOut)) {
            secKeyRing.encode(armoredOut);
        }
    }
	*/
	
    public void encryptFile(String inputFilename, String outputFilename, String base64PublicKey) {
		try {
			byte[] decodedKey = Base64.getDecoder().decode(base64PublicKey);
			InputStream inputStream = new ByteArrayInputStream(decodedKey);
			PGPPublicKey publicKey = findPublicKey(inputStream);
			try (InputStream fileIn = new FileInputStream(inputFilename);
				 OutputStream fileOut = new FileOutputStream(outputFilename);
				 OutputStream armoredOut = new ArmoredOutputStream(fileOut)) {

				// Create the PGP data encryptor
				PGPEncryptedDataGenerator encryptGen = new PGPEncryptedDataGenerator(
						new JcePGPDataEncryptorBuilder(PGPEncryptedData.CAST5)
								.setWithIntegrityPacket(true)
								.setSecureRandom(new SecureRandom())
								.setProvider("BC")
				);

				// Add the encryption method
				encryptGen.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(publicKey).setProvider("BC"));

				try (OutputStream encryptOut = encryptGen.open(armoredOut, new byte[1 << 16])) {
					// Create the compressed data generator
					PGPCompressedDataGenerator compressGen = new PGPCompressedDataGenerator(PGPCompressedData.ZIP);

					try (OutputStream compressedOut = compressGen.open(encryptOut, new byte[1 << 16])) {
						// Create the literal data generator
						PGPLiteralDataGenerator literalGen = new PGPLiteralDataGenerator();

						try (OutputStream literalOut = literalGen.open(compressedOut, PGPLiteralData.BINARY, new File(inputFilename).getName(), new Date(), new byte[1 << 16])) {
							byte[] buffer = new byte[4096];
							int bytesRead;
							while ((bytesRead = fileIn.read(buffer)) != -1) {
								literalOut.write(buffer, 0, bytesRead);
							}
						}
					}
				}
			}
		} catch (IOException e){
			System.out.println("encrypt IOException" + e.getMessage());
		} catch (PGPException e){	
			System.out.println("encrypt PGPException" + e.getMessage());		
		}
    }	

    public void decryptFile(String inputFilename, String outputFilename, String base64PrivateKey, String passwordString) {
		try {
			char [] password = passwordString.toCharArray();
			byte[] decodedKey = Base64.getDecoder().decode(base64PrivateKey);
			InputStream privateKeyIn = new ByteArrayInputStream(decodedKey);


			try (InputStream encryptedFileIn = new FileInputStream(inputFilename);
				 OutputStream decryptedOut = new FileOutputStream(outputFilename)) {

				InputStream finalEncryptedFileIn = PGPUtil.getDecoderStream(encryptedFileIn);
				PGPObjectFactory pgpFact = new PGPObjectFactory(finalEncryptedFileIn, new JcaKeyFingerprintCalculator());
				PGPEncryptedDataList encList;

				Object o = pgpFact.nextObject();
				if (o instanceof PGPEncryptedDataList) {
					encList = (PGPEncryptedDataList) o;
				} else {
					encList = (PGPEncryptedDataList) pgpFact.nextObject();
				}

				PGPPrivateKey privateKey = null;
				PGPPublicKeyEncryptedData encryptedData = null;

				for (PGPEncryptedData ed : encList) {
					PGPPublicKeyEncryptedData pked = (PGPPublicKeyEncryptedData) ed;
					privateKey = findPrivateKey(privateKeyIn, pked.getKeyID(), password);
					if (privateKey != null) {
						encryptedData = pked;
						break;
					}
				}

				if (privateKey == null) {
					throw new IllegalArgumentException("Secret key for message not found.");
				}

				try (InputStream decryptedData = encryptedData.getDataStream(new JcePublicKeyDataDecryptorFactoryBuilder().setProvider("BC").build(privateKey))) {
					PGPObjectFactory plainFact = new PGPObjectFactory(decryptedData, new JcaKeyFingerprintCalculator());
					PGPCompressedData compressedData = (PGPCompressedData) plainFact.nextObject();
					PGPObjectFactory pgpFact2 = new PGPObjectFactory(compressedData.getDataStream(), new JcaKeyFingerprintCalculator());
					PGPLiteralData literalData = (PGPLiteralData) pgpFact2.nextObject();

					try (InputStream literalIn = literalData.getInputStream()) {
						byte[] buffer = new byte[4096];
						int bytesRead;
						while ((bytesRead = literalIn.read(buffer)) != -1) {
							decryptedOut.write(buffer, 0, bytesRead);
						}
					}
				}
			}
		} catch (IOException e) {
			System.out.println("decrypt IOException" + e.getMessage());
		} catch (PGPException e) {
			System.out.println("decrypt PGPException" + e.getMessage());
		} catch (NoSuchProviderException e) {
			System.out.println("decrypt NoSuchProviderException" + e.getMessage());
		}
    }		
	
}
