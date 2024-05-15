const firebird = require('node-firebird');
const axios = require('axios');

const optionsFirebird = {
    host: 'localhost',
    port: 3050,
    database: 'C:/Users/Nathan/Downloads/AZSIMDB-MONTENEGRO.FDB',
    user: 'sysdba',
    password: 'masterkey'
};

async function fetchDataFromFirebird() {
    return new Promise((resolve, reject) => {
        firebird.attach(optionsFirebird, function (err, db) {
            if (err) {
                reject(err);
            } else {
                db.query(`SELECT FIRST 10000 le.CTX, le.TIPOCTX, le.PORTACOM, le.NREVENTO, le.STATUS, le.REFERENCIA, le.IDENTIFICACAO, le.DATAEVENTO, le.CDCLIENTE, co.CDCODIFICADOR
                FROM LOGEVENTO le
                INNER JOIN CONTRATO co ON le.CDCLIENTE = co.CDCLIENTE
                ORDER BY RAND()`, function (err, result) {

                    if (err) {
                        console.log('Resultados da consulta:', result);

                        reject(err);
                    } else {
                        console.log('Resultados da consulta:', result);
                        resolve(result);
                    }
                    db.detach();
                });
            }
        });
    });
}

async function sendDataToEndpoint(data) {
    try {
        const endpoint = 'http://localhost:8080/api/evento';

        for (const item of data) {
            await axios.post(endpoint, {
                ctx: item.CTX,
                tipoctx: item.TIPOCTX,
                portacom: item.PORTACOM,
                nrevento: item.NREVENTO,
                status: item.STATUS,
                referencia: item.REFERENCIA,
                identificacao: item.IDENTIFICACAO,
                dataevento: item.DATAEVENTO,
                codificador: item.CDCODIFICADOR
            });
            console.log('Dados enviados para o endpoint com sucesso:', item);
            // await sleep(2000);
        }
    } catch (error) {
        console.error('Erro ao enviar dados para o endpoint:', error);
    }
}

// function sleep(ms) {
//     return new Promise(resolve => setTimeout(resolve, ms));
// }

async function main() {
    try {
        const eventData = await fetchDataFromFirebird();

        await sendDataToEndpoint(eventData);
    } catch (err) {
        console.error('Erro durante o processo:', err);
    }
}

main();