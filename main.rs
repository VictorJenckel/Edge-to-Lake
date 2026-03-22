use rppal::i2c::I2c;
use std::thread;
use std::time::{Duration, Instant};
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::Write;

// =====================================================================
// PARÂMETROS DA ARQUITETURA EDGE
// =====================================================================
const MPU6050_ADDR: u16 = 0x68;
const REG_PWR_MGMT_1: u8 = 0x6B;
const REG_ACCEL_XOUT_H: u8 = 0x3B;

const TAXA_AMOSTRAGEM: usize = 500;        
const LIMITE_ANOMALIA_G: f32 = 0.35;        
const INTERVALO_LOG_SISTEMA: u64 = 3600;   

const DIRETORIO_LEITURAS: &str = "/home/victor/edgelab/leituras";

// =====================================================================
// FUNÇÃO DE DIAGNÓSTICO DO HARDWARE 
// =====================================================================
fn gerar_log_hardware(erros_i2c_hora: u32) -> String {
    
    let temp_str = fs::read_to_string("/sys/class/thermal/thermal_zone0/temp").unwrap_or_else(|_| "0".to_string());
    let temp_celsius = temp_str.trim().parse::<f32>().unwrap_or(0.0) / 1000.0;

    
    let uptime_str = fs::read_to_string("/proc/uptime").unwrap_or_else(|_| "0 0".to_string());
    let uptime_secs = uptime_str.split_whitespace().next().unwrap_or("0").parse::<f32>().unwrap_or(0.0);
    let uptime_horas = uptime_secs / 3600.0;

    
    let load_str = fs::read_to_string("/proc/loadavg").unwrap_or_else(|_| "0.00".to_string());
    let load_1min = load_str.split_whitespace().next().unwrap_or("0.00");

    
    let status_i2c = if erros_i2c_hora == 0 { "OK" } else { "FALHAS_DETECTADAS" };

    let agora = chrono::Local::now();
    let timestamp = agora.format("%Y-%m-%d %H:%M:%S").to_string();

    format!(
        "{{\"timestamp\": \"{}\", \"tipo\": \"SYSTEM_HEALTH\", \"temp_cpu_c\": {:.1}, \"cpu_load_1m\": {}, \"uptime_horas\": {:.1}, \"i2c_status\": \"{}\", \"erros_i2c_acumulados\": {}}}\n",
        timestamp, temp_celsius, load_1min, uptime_horas, status_i2c, erros_i2c_hora
    )
}

// =====================================================================
// LOOP PRINCIPAL (O Motor de Inferência)
// =====================================================================
fn main() -> Result<(), Box<dyn Error>> {
    
    fs::create_dir_all(DIRETORIO_LEITURAS).unwrap_or_else(|e| println!("Aviso: não foi possível criar o diretório: {}", e));

    let mut i2c = I2c::new()?;
    i2c.set_slave_address(MPU6050_ADDR)?;

    
    i2c.write(&[REG_PWR_MGMT_1, 0x00])?;
    println!("MPU6050 Online. Iniciando Motor de Inferência Edge a {}Hz...", TAXA_AMOSTRAGEM);

    let mut buffer_i2c = [0u8; 6];
    
    
    let mut eixo_x = [0.0f32; TAXA_AMOSTRAGEM];
    let mut eixo_y = [0.0f32; TAXA_AMOSTRAGEM];
    let mut eixo_z = [0.0f32; TAXA_AMOSTRAGEM];

    
    let mut ultimo_log_hardware = Instant::now();
    let mut contador_erros_i2c = 0;

    loop {
        
        for i in 0..TAXA_AMOSTRAGEM {
            match i2c.write_read(&[REG_ACCEL_XOUT_H], &mut buffer_i2c) {
                Ok(_) => {
                    eixo_x[i] = (((buffer_i2c[0] as i16) << 8) | (buffer_i2c[1] as i16)) as f32 / 16384.0;
                    eixo_y[i] = (((buffer_i2c[2] as i16) << 8) | (buffer_i2c[3] as i16)) as f32 / 16384.0;
                    eixo_z[i] = (((buffer_i2c[4] as i16) << 8) | (buffer_i2c[5] as i16)) as f32 / 16384.0;
                }
                Err(_) => {
                    contador_erros_i2c += 1; 
                }
            }
            
            thread::sleep(Duration::from_micros(1500)); 
        }

        
        let mut rms_x = 0.0;
        let mut rms_y = 0.0;
        let mut rms_z = 0.0;

        for eixo in [&eixo_x, &eixo_y, &eixo_z].iter().enumerate() {
            let dados = eixo.1;
            
            let media: f32 = dados.iter().sum::<f32>() / TAXA_AMOSTRAGEM as f32;
            
            
            let soma_quadrados: f32 = dados.iter().map(|&x| (x - media).powi(2)).sum();
            let rms = (soma_quadrados / TAXA_AMOSTRAGEM as f32).sqrt();

            match eixo.0 {
                0 => rms_x = rms,
                1 => rms_y = rms,
                _ => rms_z = rms,
            }
        }

        let pico_rms = rms_x.max(rms_y).max(rms_z);

        
        if pico_rms >= LIMITE_ANOMALIA_G {
            let agora = chrono::Local::now();
            let timestamp = agora.format("%Y-%m-%d %H:%M:%S").to_string();
            
            
            let nome_arquivo = format!("{}/anomalias_{}.jsonl", DIRETORIO_LEITURAS, agora.format("%Y-%m-%d"));

            println!("\n[ALERTA CRÍTICO] Anomalia detectada! RMS Máximo: {:.3}g", pico_rms);
            
            let json_line_anomalia = format!(
                "{{\"timestamp\": \"{}\", \"status\": \"ALARM\", \"rms_max\": {:.3}}}\n",
                timestamp, pico_rms
            );

            
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&nome_arquivo) {
                if let Err(e) = file.write_all(json_line_anomalia.as_bytes()) {
                    println!("Erro ao escrever anomalia no arquivo local: {}", e);
                }
            }
        }  
        
        else if ultimo_log_hardware.elapsed().as_secs() >= INTERVALO_LOG_SISTEMA {
            let log_json = gerar_log_hardware(contador_erros_i2c);
            let caminho_log = format!("{}/system_health.jsonl", DIRETORIO_LEITURAS);
            
            println!("\n>> Gravando log de máquina em: {}", caminho_log);
            
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&caminho_log) {
                let _ = file.write_all(log_json.as_bytes());
            }

            
            ultimo_log_hardware = Instant::now();
            contador_erros_i2c = 0; 
        }

        
        print!(".");
        std::io::stdout().flush().unwrap();
    }
}