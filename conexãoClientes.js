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
                db.query('SELECT DISTINCT c.*, ct.cdcontrato, s.numsetor, s.local FROM CLIENTE c LEFT JOIN CONTRATO ct ON c.cdcliente = ct.cdcliente LEFT JOIN SETOR s ON ct.cdcontrato = s.cdcontrato', function (err, result) {
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
        const contatos = [];
        for (let i = 1; i <= 6; i++) {
            if (item[`FONE${i}`]) {
                contatos.push({
                    nome: item[`FONEOBS${i}`] || "",
                    telefone: item[`FONE${i}`] || "",
                    senha: "",
                    contraSenha: "",
                    observacao: ""
                });
            }
        }

        // Se não houver contatos, adiciona uma entrada vazia
        if (contatos.length === 0) {
            contatos.push({
                nome: "",
                telefone: "",
                senha: "",
                contraSenha: "",
                observacao: ""
            });
        }

        let natureza;
        const tpPessoa = item.TPPESSOA && item.TPPESSOA.trim().toLowerCase();
        if (tpPessoa === "j") {
            natureza = "JURIDICA";
        } else if (tpPessoa === "f") {
            natureza = "FISICA";
        } else {
            natureza = 'OUTRO';
        }

        return {
            unidade: 'MONTENEGRO',
            codHabil: item.codHabil,
            codCondor: item.codCondor,
            natureza: natureza,
            documento: item.DOCUMENTO,
            inscMunincipal: item.INSCMUNICIPAL,
            ativo: true,
            nome: item.NMCLIENTE,
            endereco: item.ENDERECO,
            bairro: item.BAIRRO,
            cidade: item.CIDADE,
            uf: item.UF,
            cep: item.CEP,
            observacao: item.OBSERVACAO,
            contatos: contatos,
            cdcontrato: item.CDCONTRATO, // Incluindo o cdcontrato
            setores: [{
                setor: item.NUMSETOR || "",
                localizacao: item.LOCAL || "",
                observacao: ""
            }],
            viagens: [{
                nomeContatoNotificacaoSaida: "",
                nomeContatoNotificacaoVolta: "",
                observacao: "",
                procedimento: ""
            }]
        };
    });
}


async function main() {
    try {
        const clienteData = await fetchDataFromFirebird();

        // Modificar os dados antes de salvar no MongoDB
        const modifiedData = await modifyData(clienteData);

        // Verificar se o cliente já foi salvo com base no nome
        const uniqueClients = modifiedData.filter((item, index, self) =>
            index === self.findIndex((t) => t.nome === item.nome)
        );

        // Salvar os dados únicos no MongoDB
        await saveDataToMongoDB(uniqueClients);
    } catch (err) {
        console.error('Erro durante o processo:', err);
    }
}



async function saveDataToMongoDB(data) {
    const client = new MongoClient('mongodb://localhost:27017/azsimdb', optionsMongoDB);

    try {
        await client.connect();
        const db = client.db();
        const collection = db.collection('cliente');

        await collection.insertMany(data);
        console.log('Dados salvos no MongoDB com sucesso!');
    } catch (err) {
        console.error('Erro ao salvar dados no MongoDB:', err);
    } finally {
        await client.close();
    }
}

main();
