const firebird = require('node-firebird');
const { MongoClient } = require('mongodb');

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
                db.query(`
                    SELECT DISTINCT 
                        c.*, 
                        ct.cdcontrato, 
                        s.numsetor, 
                        s.local, 
                        cc.cdcodificador 
                    FROM 
                        CLIENTE c 
                        LEFT JOIN CONTRATO ct ON c.cdcliente = ct.cdcliente 
                        LEFT JOIN SETOR s ON ct.cdcontrato = s.cdcontrato 
                        LEFT JOIN CONTRATO cc ON ct.cdcontrato = cc.cdcontrato
                `, function (err, result) {
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

function groupDataByCdContrato(data) {
    const groupedData = {};

    data.forEach(item => {
        if (!groupedData[item.CDCONTRATO]) {
            groupedData[item.CDCONTRATO] = {
                ...item,
                setores: []
            };
        }

        const setor = item.NUMSETOR ? { setor: item.NUMSETOR } : null;
        const localizacao = item.LOCAL ? { localizacao: item.LOCAL } : null;
        const observacao = item.OBSERVACAO ? { observacao: item.OBSERVACAO } : null;

        if (setor || localizacao || observacao) {
            groupedData[item.CDCONTRATO].setores.push({
                ...setor,
                ...localizacao,
                ...observacao
            });
        }
    });

    return Object.values(groupedData);
}

async function modifyData(data) {
    const groupedData = groupDataByCdContrato(data);

    return Promise.all(groupedData.map(async (item) => {
        const contatos = [];
        for (let i = 1; i <= 6; i++) {
            if (item[`FONE${i}`]) {
                const nome = item[`FONEOBS${i}`] ? { nome: item[`FONEOBS${i}`] } : null;
                contatos.push({
                    telefone: item[`FONE${i}`],
                    ...nome,
                    senha: "",
                    contraSenha: "",
                    observacao: ""
                });
            }
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
            contatos: contatos.length > 0 ? contatos : [],
            cdcontrato: item.CDCONTRATO,
            codificador: item.CDCODIFICADOR,
            setores: item.setores.map(setor => {
                const novoSetor = {};
                if (setor.setor) novoSetor.setor = setor.setor;
                if (setor.localizacao) novoSetor.localizacao = setor.localizacao;
                if (setor.observacao) novoSetor.observacao = setor.observacao;
                return novoSetor;
            }),
            viagens: []
        };
    }));
}

async function saveDataToMongoDB(data) {
    const client = new MongoClient('mongodb://dhxqhp_azsimdb:D4qQwl2IAv@mongodb-ag-br1-2.conteige.cloud:27017/dhxqhp_azsimdb?authMechanism=DEFAULT&tls=false&authSource=dhxqhp_azsimdb', optionsMongoDB);

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

async function main() {
    try {
        const clienteData = await fetchDataFromFirebird();
        const modifiedData = await modifyData(clienteData);
        const uniqueClients = modifiedData.filter((item, index, self) =>
            index === self.findIndex((t) => t.nome === item.nome)
        );
        await saveDataToMongoDB(uniqueClients);
    } catch (err) {
        console.error('Erro durante o processo:', err);
    }
}

main();
