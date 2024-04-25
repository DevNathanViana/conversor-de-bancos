const firebird = require('node-firebird');
const MongoClient = require('mongodb').MongoClient;

const optionsFirebird = {
    host: 'localhost',
    port: 3050,
    database: 'C:/Users/Nathan/Downloads/AZSIMDB-MONTENEGRO.FDB',
    user: 'sysdba',
    password: 'masterkey'
};

const optionsMongoDB = {
    useNewUrlParser: true,
    useUnifiedTopology: true
};

async function fetchDataFromFirebird() {
    return new Promise((resolve, reject) => {
        firebird.attach(optionsFirebird, function (err, db) {
            if (err) {
                reject(err);
            } else {
                db.query('SELECT *  FROM STATUS', function (err, result) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(result);
                    }
                    db.detach();
                });
            }
        });
    });
}

async function modifyData(data) {
    return data.map(item => {

        return {
            sts: item.ESTATUS,
            referencia1: item.REFERENCIA1,
            referencia2: item.REFERENCIA2,
            setor: item.SETOR,
            grupo: item.GRUPO,
            alarme: item.ALARME,
            mensagem: item.MENSAGEM,
            monitor: item.MONITOR,
            identificacao: item.IDENTIFICACAO,
            ocorrencia: item.OCORRENCIA,
            descricao: item.DESCRICAO,
            cor: item.COR,

        };
    });
}


async function main() {
    try {
        const clienteData = await fetchDataFromFirebird();

        // Modificar os dados antes de salvar no MongoDB
        const modifiedData = await modifyData(clienteData);

        // Salvar os dados únicos no MongoDB
        await saveDataToMongoDB(modifiedData);
    } catch (err) {
        console.error('Erro durante o processo:', err);
    }
}



async function saveDataToMongoDB(data) {
    const client = new MongoClient('mongodb://localhost:27017/status', optionsMongoDB);

    try {
        await client.connect();
        const db = client.db();
        const collection = db.collection('config_evento');

        await collection.insertMany(data);
        console.log('Dados salvos no MongoDB com sucesso!');
    } catch (err) {
        console.error('Erro ao salvar dados no MongoDB:', err);
    } finally {
        await client.close();
    }
}

main();
